package terraform

import (
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/addrs"
	"github.com/hashicorp/terraform/configs"

	"github.com/hashicorp/terraform/dag"
)

// ConcreteProviderNodeFunc is a callback type used to convert an
// abstract provider to a concrete one of some type.
type ConcreteProviderNodeFunc func(*NodeAbstractProvider) dag.Vertex

// NodeAbstractProvider represents a provider that has no associated operations.
// It registers all the common interfaces across operations for providers.
type NodeAbstractProvider struct {
	NameValue string
	PathValue addrs.ModuleInstance

	// The fields below will be automatically set using the Attach
	// interfaces if you're running those transforms, but also be explicitly
	// set if you already have that information.

	Config *configs.Provider
}

func ResolveProviderName(name string, path addrs.ModuleInstance) string {
	if strings.Contains(name, "provider.") {
		// already resolved
		return name
	}

	name = fmt.Sprintf("provider.%s", name)
	if len(path) >= 1 {
		name = fmt.Sprintf("%s.%s", modulePrefixStr(path), name)
	}

	return name
}

func (n *NodeAbstractProvider) Name() string {
	return ResolveProviderName(n.NameValue, n.PathValue)
}

// GraphNodeSubPath
func (n *NodeAbstractProvider) Path() addrs.ModuleInstance {
	return n.PathValue
}

// RemovableIfNotTargeted
func (n *NodeAbstractProvider) RemoveIfNotTargeted() bool {
	// We need to add this so that this node will be removed if
	// it isn't targeted or a dependency of a target.
	return true
}

// GraphNodeReferencer
func (n *NodeAbstractProvider) References() []string {
	if n.Config == nil {
		return nil
	}

	return ReferencesFromConfig(n.Config.RawConfig)
}

// GraphNodeProvider
func (n *NodeAbstractProvider) ProviderName() string {
	return n.NameValue
}

// GraphNodeProvider
func (n *NodeAbstractProvider) ProviderConfig() *configs.Provider {
	if n.Config == nil {
		return nil
	}

	return n.Config
}

// GraphNodeAttachProvider
func (n *NodeAbstractProvider) AttachProvider(c *configs.Provider) {
	n.Config = c
}

// GraphNodeDotter impl.
func (n *NodeAbstractProvider) DotNode(name string, opts *dag.DotOpts) *dag.DotNode {
	return &dag.DotNode{
		Name: name,
		Attrs: map[string]string{
			"label": n.Name(),
			"shape": "diamond",
		},
	}
}
