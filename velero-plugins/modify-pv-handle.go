package main

import (
	"context"
	"os"
	"regexp"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
)

// ModifyVolumeHandleAction holds our logger and k8s client
type ModifyVolumeHandleAction struct {
	Log       logrus.FieldLogger
	k8sClient kubernetes.Interface
}

// ConfigMap settings
const (
	configMapName      = "velero-plugin-regex-map"
	veleroNamespaceEnv = "VELERO_NAMESPACE"
	findKey            = "regex.find"
	replaceKey         = "regex.replace"
)

// NewModifyVolumeHandleAction (constructor)
// We need to re-add the Kubernetes client initialization
func NewModifyVolumeHandleAction(logger logrus.FieldLogger) (interface{}, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &ModifyVolumeHandleAction{
		Log:       logger,
		k8sClient: client,
	}, nil
}

// AppliesTo defines that this plugin only runs on PersistentVolumes
func (p *ModifyVolumeHandleAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"persistentvolumes"},
	}, nil
}

// Execute is the core logic
func (p *ModifyVolumeHandleAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	p.Log.Info("Starting ModifyVolumeHandleAction (ConfigMap Regex) for PV...")

	// 1. Get the ConfigMap with regex patterns
	namespace := os.Getenv(veleroNamespaceEnv)
	if namespace == "" {
		namespace = "velero" // Default
	}

	cm, err := p.k8sClient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), configMapName, metav1.GetOptions{})
	if err != nil {
		p.Log.Errorf("Failed to get ConfigMap %s: %v. Skipping regex replacement.", configMapName, err)
		// If map doesn't exist, just return the item unmodified
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}

	// 2. Get the find and replace strings from the ConfigMap
	findPattern, okFind := cm.Data[findKey]
	replacePattern, okReplace := cm.Data[replaceKey]

	if !okFind || !okReplace {
		p.Log.Warnf("ConfigMap %s is missing '%s' or '%s' keys. Skipping regex replacement.", configMapName, findKey, replaceKey)
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}

	// 3. Compile the regex
	searchRegex, err := regexp.Compile(findPattern)
	if err != nil {
		p.Log.Errorf("Invalid 'regex.find' pattern in ConfigMap: %v. Skipping.", err)
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}

	// 4. Convert item to a PersistentVolume
	pv := new(corev1.PersistentVolume)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(input.Item.UnstructuredContent(), pv); err != nil {
		return nil, err
	}

	// 5. Skip if not a CSI volume or handle is empty
	if pv.Spec.CSI == nil {
		p.Log.Info("Skipping PV: not a CSI volume.")
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}
	oldHandle := pv.Spec.CSI.VolumeHandle
	if oldHandle == "" {
		p.Log.Info("Skipping PV: CSI VolumeHandle is empty.")
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}

	// 6. Perform the replacement
	if searchRegex.MatchString(oldHandle) {
		newHandle := searchRegex.ReplaceAllString(oldHandle, replacePattern)

		p.Log.Infof("Regex match found in PV %s: changing VolumeHandle from '%s' to '%s'", pv.Name, oldHandle, newHandle)
		pv.Spec.CSI.VolumeHandle = newHandle

		// 7. Convert modified PV back to Unstructured
		newItem, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pv)
		if err != nil {
			return nil, err
		}
		input.Item.SetUnstructuredContent(newItem)

		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}

	p.Log.Infof("Regex pattern not found in VolumeHandle: '%s'. Restoring as-is.", oldHandle)
	return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
}
