package sign

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"emperror.dev/errors"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	sigsyaml "sigs.k8s.io/yaml"
)

var log = logf.Log.WithName("signer_sign_cmd")

var f, publickey, privatekey, privatekeypassword string
var local, upload bool
var retry int

var SignCmd = &cobra.Command{
	Use:   "sign",
	Short: "Sign the yaml",
	Long:  `Sign the yaml. Takes yaml file, public key and private key as args`,
	Run: func(cmd *cobra.Command, args []string) {

		if publickey == "" {
			log.Error(errors.New("public key not provided"), "public key not provided")
			os.Exit(1)
		}

		if privatekey == "" {
			log.Error(errors.New("private key not provided"), "private key not provided")
			os.Exit(1)
		}

		var file io.ReadCloser
		var err error
		if isInputFromPipe() {
			file = os.Stdin
		} else {
			file, err = openInputFile(f)
			if err != nil {
				log.Error(err, "Could not open input file")
				os.Exit(1)
			}
		}

		uobjs, err := Decode(file)
		file.Close()
		if err != nil {
			log.Error(err, "Could not Decode input yaml contents")
			os.Exit(1)
		}

		pubKey, err := ioutil.ReadFile(publickey)
		if err != nil {
			log.Error(err, "Could not read public key file")
			os.Exit(1)
		}

		password := ""
		if privatekeypassword != "" {
			password = privatekeypassword
		}

		privKey, err := PrivateKeyFromPemFile(privatekey, password)
		if err != nil {
			log.Error(err, "Could not get private key from pem file")
			os.Exit(1)
		}

		for i, uobj := range uobjs {
			// Reduce Object to GVK+Spec and sign that content
			bytes, err := UnstructuredToGVKSpecBytes(uobj)
			if err != nil {
				log.Error(err, "could not MarshalJSON")
				os.Exit(1)
			}

			bytesHash := sha256.New()
			_, err = bytesHash.Write(bytes)
			if err != nil {
				log.Error(err, "could not hash")
				os.Exit(1)
			}
			bytesHashSum := bytesHash.Sum(nil)

			signature, err := rsa.SignPSS(rand.Reader, privKey, crypto.SHA256, bytesHashSum, nil)
			if err != nil {
				log.Error(err, "could not sign")
				os.Exit(1)
			}

			annotations := make(map[string]string)

			annotations["marketplace.redhat.com/signature"] = fmt.Sprintf("%x", signature)
			annotations["marketplace.redhat.com/publickey"] = fmt.Sprintf("%s", pubKey)

			uobjs[i].SetAnnotations(annotations)
		}

		uList := unstructured.UnstructuredList{}
		uList.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "List",
		})
		uList.Items = uobjs

		uListBytes, err := uList.MarshalJSON()
		if err != nil {
			log.Error(err, "could not MarshalJSON")
			os.Exit(1)
		}

		yamlout, err := sigsyaml.JSONToYAML(uListBytes)
		if err != nil {
			log.Error(err, "could not JSONToYAML")
			os.Exit(1)
		}

		fmt.Printf("%s", yamlout)

		os.Exit(0)
	},
}

func Decode(reader io.Reader) ([]unstructured.Unstructured, error) {
	decoder := yaml.NewYAMLToJSONDecoder(reader)
	objs := []unstructured.Unstructured{}
	var err error
	for {
		out := unstructured.Unstructured{}
		err = decoder.Decode(&out)
		if err == io.EOF {
			break
		}
		if err != nil || len(out.Object) == 0 {
			continue
		}
		objs = append(objs, out)
	}
	if err != io.EOF {
		return nil, err
	}
	return objs, nil
}

func PrivateKeyFromPemFile(privateKeyFile string, privateKeyPassword string) (*rsa.PrivateKey, error) {

	privPEMData, err := ioutil.ReadFile(privateKeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to read private key file")
	}

	pemblock, _ := pem.Decode(privPEMData)
	if pemblock == nil || pemblock.Type != "RSA PRIVATE KEY" {
		return nil, errors.Wrap(err, "Unable to decode private key")
	}

	var pemBlockBytes []byte
	if privateKeyPassword != "" {
		pemBlockBytes, err = x509.DecryptPEMBlock(pemblock, []byte(privateKeyPassword))
	} else {
		pemBlockBytes = pemblock.Bytes
	}

	var parsedPrivateKey interface{}
	if parsedPrivateKey, err = x509.ParsePKCS1PrivateKey(pemBlockBytes); err != nil {
		if parsedPrivateKey, err = x509.ParsePKCS8PrivateKey(pemBlockBytes); err != nil {
			return nil, errors.Wrap(err, "Unable to parse private key")
		}
	}

	var privateKey *rsa.PrivateKey
	var ok bool
	privateKey, ok = parsedPrivateKey.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.Wrap(err, "Unable to parse private key")
	}

	return privateKey, nil
}

// This should be the bytes we sign, or verify signature on
func UnstructuredToGVKSpecBytes(uobj unstructured.Unstructured) ([]byte, error) {
	gvkspecuobj := unstructured.Unstructured{}
	gvkspecuobj.SetGroupVersionKind(uobj.GroupVersionKind())
	gvkspecuobj.Object["spec"] = uobj.Object["spec"]

	bytes, err := gvkspecuobj.MarshalJSON()
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func isInputFromPipe() bool {
	info, _ := os.Stdin.Stat()
	return info.Mode()&os.ModeCharDevice == 0
}

func openInputFile(in string) (io.ReadCloser, error) {
	info, err := os.Stat(in)
	if err != nil {
		return nil, err
	}

	if info.IsDir() {
		return nil, errors.New("input file is directory, not file")
	}

	file, err := os.Open(in)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func init() {
	SignCmd.Flags().StringVar(&f, "f", "", "input yaml file")
	SignCmd.Flags().StringVar(&publickey, "publickey", "", "public key")
	SignCmd.Flags().StringVar(&privatekey, "privatekey", "", "private key")
	SignCmd.Flags().StringVar(&privatekeypassword, "privatekeypassword", "", "private key password")
}
