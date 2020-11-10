// Copyright 2020 IBM Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package harness

import (
	"context"
	"io/ioutil"

	"emperror.dev/errors"
	"github.com/caarlos0/env/v6"
	monitoringv1 "github.com/coreos/prometheus-operator/pkg/apis/monitoring/v1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

type mockOpenShift struct {
	Namespace       string   `env:"NAMESPACE" envDefault:"openshift-redhat-marketplace"`

	cleanup []runtime.Object
}

func (e *mockOpenShift) Name() string {
	return "MockOpenShift"
}

func (e *mockOpenShift) Parse() error {
	return env.Parse(e)
}

func (e *mockOpenShift) HasCleanup() []runtime.Object {
	return e.cleanup
}

func (e *mockOpenShift) Setup(h *TestHarness) error {
	additionalNamespaces := []string{
		"openshift-monitoring",
		"openshift-config-managed",
		"openshift-config",
	}

	for _, namespace := range additionalNamespaces {
		ok, err := SucceedOrAlreadyExist.Match(
			h.Client.Create(context.TODO(), &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: namespace,
				},
			}))

		if !ok {
			return errors.Wrapf(err, "failed to create namespace: %s", namespace)
		}
	}

	var (
		kubeletMonitor = &monitoringv1.ServiceMonitor{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "kubelet",
				Namespace: "openshift-monitoring",
			},
			Spec: monitoringv1.ServiceMonitorSpec{
				Endpoints: []monitoringv1.Endpoint{
					{
						Port: "web",
					},
				},
				Selector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						"foo": "bar",
					},
				},
			},
		}

		kubeStateMonitor = &monitoringv1.ServiceMonitor{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "kube-state-metrics",
				Namespace: "openshift-monitoring",
			},
			Spec: monitoringv1.ServiceMonitorSpec{
				Endpoints: []monitoringv1.Endpoint{
					{
						Port: "web",
					},
				},
				Selector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						"foo": "bar",
					},
				},
			},
		}
	)

	e.cleanup = []runtime.Object{kubeletMonitor, kubeStateMonitor}

	Expect(h.Client.Create(context.TODO(), kubeletMonitor)).To(SucceedOrAlreadyExist)
	Expect(h.Client.Create(context.TODO(), kubeStateMonitor)).To(SucceedOrAlreadyExist)

	var (
		tlsResources = []types.NamespacedName{
			{Namespace: h.config.Namespace, Name: "prometheus-operator-tls"},
			{Namespace: h.config.Namespace, Name: "rhm-metric-state-tls"},
			{Namespace: h.config.Namespace, Name: "rhm-prometheus-meterbase-tls"},
		}
		certsResources = []types.NamespacedName{
			{Namespace: "openshift-config-managed", Name: "kubelet-serving-ca"},
			{Namespace: h.config.Namespace, Name: "serving-certs-ca-bundle"},
			{Namespace: h.config.Namespace, Name: "operator-certs-ca-bundle"},
		}
	)

	serviceCA, err := ioutil.ReadFile("../certs/kubelet-client-current.pem")
	Expect(err).To(Succeed())

	for _, resource := range certsResources {
		configmap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: resource.Namespace,
				Name:      resource.Name,
				Annotations: map[string]string{
					"service.beta.openshift.io/inject-cabundle": "true",
				},
			},
			Data: map[string]string{
				"service-ca.crt": string(serviceCA),
			},
		}

		Expect(h.Create(context.TODO(), configmap)).Should(SucceedOrAlreadyExist, "failed to create", "name", resource.Name)
		e.cleanup = append(e.cleanup, configmap)
	}

	serverCrt, err := ioutil.ReadFile("../certs/kubelet.crt")
	Expect(err).To(Succeed())
	serverKey, err := ioutil.ReadFile("../certs/kubelet.key")
	Expect(err).To(Succeed())

	for _, resource := range tlsResources {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: resource.Namespace,
				Name:      resource.Name,
			},
			Type: corev1.SecretTypeTLS,
			Data: map[string][]byte{
				"tls.crt": serverCrt,
				"tls.key": serverKey,
			},
		}

		Expect(h.Create(context.TODO(), secret)).Should(SucceedOrAlreadyExist, "failed to create", "name", resource.Name)
		e.cleanup = append(e.cleanup, secret)
	}

	return nil
}
