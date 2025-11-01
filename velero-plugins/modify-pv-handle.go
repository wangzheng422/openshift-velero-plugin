package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
)

// ReplacementRule defines a single replacement operation
type ReplacementRule struct {
	Path    string
	Find    string
	Replace string
}

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
	p.Log.Info("Starting ModifyPVFieldsAction for PV...")

	// 1. Get the ConfigMap with regex rules
	namespace := os.Getenv(veleroNamespaceEnv)
	if namespace == "" {
		namespace = "velero" // Default
	}

	cm, err := p.k8sClient.CoreV1().ConfigMaps(namespace).Get(context.TODO(), configMapName, metav1.GetOptions{})
	if err != nil {
		p.Log.Errorf("Failed to get ConfigMap %s: %v. Skipping modifications.", configMapName, err)
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}

	// 2. Parse rules from the ConfigMap
	rules := p.parseRulesFromConfigMap(cm.Data)
	if len(rules) == 0 {
		p.Log.Warnf("No valid rules found in ConfigMap %s. Skipping modifications.", configMapName)
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}

	// 3. Get the unstructured content of the PV
	pv, ok := input.Item.UnstructuredContent()["spec"].(map[string]interface{})
	if !ok {
		p.Log.Warn("Could not cast PV spec to map[string]interface{}. Skipping.")
		return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
	}
	pvName := input.Item.UnstructuredContent()["metadata"].(map[string]interface{})["name"].(string)

	// 4. Iterate through rules and apply them
	modified := false
	for _, rule := range rules {
		p.Log.Infof("Applying rule for path: %s", rule.Path)
		pathParts := strings.Split(rule.Path, ".")

		// We are working within the 'spec' field, so remove it if present
		if len(pathParts) > 0 && pathParts[0] == "spec" {
			pathParts = pathParts[1:]
		}

		currentVal, found, err := unstructured.NestedString(pv, pathParts...)
		if err != nil {
			p.Log.Errorf("Error accessing path %s for PV %s: %v. Skipping rule.", rule.Path, pvName, err)
			continue
		}
		if !found {
			p.Log.Warnf("Path %s not found for PV %s. Skipping rule.", rule.Path, pvName)
			continue
		}

		// Compile regex for the current rule
		searchRegex, err := regexp.Compile(rule.Find)
		if err != nil {
			p.Log.Errorf("Invalid 'find' pattern for path %s: %v. Skipping rule.", rule.Path, err)
			continue
		}

		// Perform replacement
		if searchRegex.MatchString(currentVal) {
			newVal := searchRegex.ReplaceAllString(currentVal, rule.Replace)
			p.Log.Infof("PV %s: Match found for path %s. Changing from '%s' to '%s'", pvName, rule.Path, currentVal, newVal)

			if err := unstructured.SetNestedField(pv, newVal, pathParts...); err != nil {
				p.Log.Errorf("Error setting path %s for PV %s: %v. Skipping rule.", rule.Path, pvName, err)
				continue
			}
			modified = true
		} else {
			p.Log.Infof("PV %s: No regex match for path %s on value '%s'.", pvName, rule.Path, currentVal)
		}
	}

	if modified {
		p.Log.Infof("PV %s was modified.", pvName)
	} else {
		p.Log.Infof("PV %s was not modified.", pvName)
	}

	return velero.NewRestoreItemActionExecuteOutput(input.Item), nil
}

// parseRulesFromConfigMap extracts and sorts rules from ConfigMap data
func (p *ModifyVolumeHandleAction) parseRulesFromConfigMap(data map[string]string) []ReplacementRule {
	rulesMap := make(map[string]ReplacementRule)

	for key, value := range data {
		parts := strings.Split(key, ".")
		if len(parts) != 2 {
			continue // Expecting format like "rule1.path"
		}
		ruleID := parts[0]
		attr := parts[1]

		rule := rulesMap[ruleID]
		switch attr {
		case "path":
			rule.Path = value
		case "find":
			rule.Find = value
		case "replace":
			rule.Replace = value
		}
		rulesMap[ruleID] = rule
	}

	var rules []ReplacementRule
	for _, rule := range rulesMap {
		if rule.Path != "" && rule.Find != "" { // 'replace' can be empty
			rules = append(rules, rule)
		} else {
			p.Log.Warnf("Incomplete rule found, skipping. A valid rule must have 'path' and 'find' keys. Found: path=%s, find=%s", rule.Path, rule.Find)
		}
	}

	// Sort rules by key (rule1, rule2, etc.) to ensure consistent order
	sort.Slice(rules, func(i, j int) bool {
		// Extract numeric part of the rule ID for proper sorting
		id_i := -1
		fmt.Sscanf(rules[i].Path, "rule%d", &id_i)
		id_j := -1
		fmt.Sscanf(rules[j].Path, "rule%d", &id_j)
		return id_i < id_j
	})

	return rules
}
