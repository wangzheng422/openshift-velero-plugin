package main

import (
	"context"
	"fmt"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v7/apis/volumesnapshot/v1"
	"github.com/sirupsen/logrus"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// CreatePvcFromSnapshotAction plugin for Velero
type CreatePvcFromSnapshotAction struct {
	Log       logrus.FieldLogger
	k8sClient kubernetes.Interface
}

// NewCreatePvcFromSnapshotAction instantiates the plugin.
func NewCreatePvcFromSnapshotAction(logger logrus.FieldLogger) (interface{}, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %v", err)
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %v", err)
	}

	return &CreatePvcFromSnapshotAction{
		Log:       logger,
		k8sClient: client,
	}, nil
}

// AppliesTo defines the resources that this action will apply to.
func (p *CreatePvcFromSnapshotAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshots.snapshot.storage.k8s.io"},
	}, nil
}

// Execute is the core logic of the action.
func (p *CreatePvcFromSnapshotAction) Execute(item runtime.Unstructured, backup *velerov1api.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, error) {
	p.Log.Info("Starting CreatePvcFromSnapshotAction for VolumeSnapshot...")

	var snap snapshotv1.VolumeSnapshot
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), &snap); err != nil {
		return nil, nil, fmt.Errorf("failed to convert unstructured to VolumeSnapshot: %v", err)
	}

	p.Log.Infof("Processing VolumeSnapshot %s/%s", snap.Namespace, snap.Name)

	// 1. Get the source PVC name from the snapshot
	if snap.Spec.Source.PersistentVolumeClaimName == nil {
		p.Log.Infof("VolumeSnapshot %s/%s does not have a source PVC name, skipping.", snap.Namespace, snap.Name)
		return item, nil, nil
	}
	sourcePvcName := *snap.Spec.Source.PersistentVolumeClaimName
	p.Log.Infof("Source PVC for snapshot is %s/%s", snap.Namespace, sourcePvcName)

	// 2. Get the original PVC to copy its spec
	sourcePvc, err := p.k8sClient.CoreV1().PersistentVolumeClaims(snap.Namespace).Get(context.TODO(), sourcePvcName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			p.Log.Warnf("Source PVC %s/%s not found, cannot create a new PVC from its snapshot. Skipping.", snap.Namespace, sourcePvcName)
			return item, nil, nil
		}
		p.Log.Errorf("Failed to get source PVC %s/%s: %v", snap.Namespace, sourcePvcName, err)
		return nil, nil, err // Return error to fail the backup for this item
	}

	// 3. Define the new PVC to be created
	newPvcName := fmt.Sprintf("%s-restored-%s", snap.Name, backup.Name)
	apiGroup := "snapshot.storage.k8s.io"
	newPvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      newPvcName,
			Namespace: snap.Namespace,
			Annotations: map[string]string{
				"velero.io/created-by-plugin": "create-pvc-from-snapshot",
				"velero.io/source-snapshot":   fmt.Sprintf("%s/%s", snap.Namespace, snap.Name),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      sourcePvc.Spec.AccessModes,
			Resources:        sourcePvc.Spec.Resources,
			StorageClassName: sourcePvc.Spec.StorageClassName,
			DataSource: &corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     "VolumeSnapshot",
				Name:     snap.Name,
			},
		},
	}

	p.Log.Infof("Attempting to create new PVC %s/%s from snapshot %s", newPvc.Namespace, newPvc.Name, snap.Name)

	// 4. Create the new PVC in the cluster
	_, err = p.k8sClient.CoreV1().PersistentVolumeClaims(newPvc.Namespace).Create(context.TODO(), newPvc, metav1.CreateOptions{})
	if err != nil {
		p.Log.Errorf("Failed to create new PVC %s/%s: %v", newPvc.Namespace, newPvc.Name, err)
		// Don't fail the whole backup for this item, just log the error and continue
		return item, nil, nil
	}

	p.Log.Infof("Successfully created new PVC %s/%s.", newPvc.Namespace, newPvc.Name)

	// 5. Add the new PVC to the list of items to be backed up
	additionalItem := velero.ResourceIdentifier{
		GroupResource: schema.GroupResource{Group: "", Resource: "persistentvolumeclaims"},
		Namespace:     newPvc.Namespace,
		Name:          newPvc.Name,
	}

	p.Log.Infof("Returning original VolumeSnapshot and adding new PVC %s to the backup.", newPvc.Name)
	return item, []velero.ResourceIdentifier{additionalItem}, nil
}
