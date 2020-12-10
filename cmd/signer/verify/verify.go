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

package verify

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"emperror.dev/errors"
	"github.com/redhat-marketplace/redhat-marketplace-operator/cmd/signer/util"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("signer_verify_cmd")

var f, ca string

var VerifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify the yaml",
	Long:  `Verify the yaml. Takes yaml file, ca as args`,
	Run: func(cmd *cobra.Command, args []string) {

		if ca == "" {
			log.Error(errors.New("ca not provided"), "ca not provided")
			os.Exit(1)
		}

		var file io.ReadCloser
		var err error
		if util.IsInputFromPipe() {
			file = os.Stdin
		} else {
			file, err = util.OpenInputFile(f)
			if err != nil {
				log.Error(err, "Could not open input file")
				os.Exit(1)
			}
		}

		uobjs, err := util.Decode(file)
		file.Close()
		if err != nil {
			log.Error(err, "Could not Decode input yaml contents")
			os.Exit(1)
		}

		caCert, err := util.CertificateFromPemFile(ca)
		if err != nil {
			log.Error(err, "Could not retrieve ca certificate")
			os.Exit(1)
		}

		err = verify(uobjs, caCert)
		if err != nil {
			fmt.Printf("yaml failed verification")
			log.Error(err, "yaml failed verification")
			os.Exit(1)
		}

		os.Exit(0)
	},
}

func verify(uobjs []unstructured.Unstructured, caCert *x509.Certificate) error {
	for _, uobj := range uobjs {
		if uobj.IsList() {
			uobjList, err := uobj.ToList()
			if err != nil {
				return err
			}
			return verify(uobjList.Items, caCert)
		} else {

			annotations := uobj.GetAnnotations()

			// verify pubCert against caCert

			pubKeyBytes := []byte(annotations["marketplace.redhat.com/publickey"])

			pubCert, err := util.CertificateFromPemBytes(pubKeyBytes)
			if err != nil {
				return errors.Wrap(err, "public key annotation is malformed")
			}

			util.VerifyCert(caCert, pubCert)
			if err != nil {
				return errors.Wrap(err, "failed to verify public certificate against ca certificate")
			}

			// verify content & signature

			// Reduce Object to GVK+Spec
			bytes, err := util.UnstructuredToGVKSpecBytes(uobj)
			if err != nil {
				return errors.Wrap(err, "could not MarshalJSON")
			}
			hash := sha256.Sum256(bytes)

			signaturestring := annotations["marketplace.redhat.com/signature"]
			signaturehex, err := hex.DecodeString(signaturestring)
			if err != nil {
				return errors.Wrap(err, "signature is malformed, can not hex decode")
			}

			var rsaPublicKey *rsa.PublicKey
			var ok bool
			rsaPublicKey, ok = pubCert.PublicKey.(*rsa.PublicKey)
			if !ok {
				return errors.Wrap(err, "Unable to parse public key")
			}

			err = rsa.VerifyPSS(rsaPublicKey, crypto.SHA256, hash[:], signaturehex, nil)
			if err != nil {
				return errors.Wrap(err, "failed to VerifyPSS")
			}
		}
	}
	return nil
}

func init() {
	VerifyCmd.Flags().StringVar(&f, "f", "", "input yaml file")
	VerifyCmd.Flags().StringVar(&ca, "ca", "", "certificate authority file")
}
