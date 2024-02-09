package common

import (
	"net/http"
	"strings"

	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/schema"
	metricsStore "github.com/rancher/steve/pkg/stores/metrics"
	"github.com/rancher/steve/pkg/stores/proxy"
	"github.com/rancher/steve/pkg/summarycache"
	"github.com/rancher/wrangler/v2/pkg/data"
	corecontrollers "github.com/rancher/wrangler/v2/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/v2/pkg/summary"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
)

func DefaultTemplate(clientGetter proxy.ClientGetter,
	summaryCache *summarycache.SummaryCache,
	asl accesscontrol.AccessSetLookup,
	namespaceCache corecontrollers.NamespaceCache) schema.Template {
	return schema.Template{
		Store:     metricsStore.NewMetricsStore(proxy.NewProxyStore(clientGetter, summaryCache, asl, namespaceCache)),
		Formatter: formatter(summaryCache, asl),
	}
}

func selfLink(gvr schema2.GroupVersionResource, meta metav1.Object) (prefix string) {
	buf := &strings.Builder{}
	if gvr.Group == "management.cattle.io" && gvr.Version == "v3" {
		buf.WriteString("/v1/")
		buf.WriteString(gvr.Group)
		buf.WriteString(".")
		buf.WriteString(gvr.Resource)
		if meta.GetNamespace() != "" {
			buf.WriteString("/")
			buf.WriteString(meta.GetNamespace())
		}
	} else {
		if gvr.Group == "" {
			buf.WriteString("/api/v1/")
		} else {
			buf.WriteString("/apis/")
			buf.WriteString(gvr.Group)
			buf.WriteString("/")
			buf.WriteString(gvr.Version)
			buf.WriteString("/")
		}
		if meta.GetNamespace() != "" {
			buf.WriteString("namespaces/")
			buf.WriteString(meta.GetNamespace())
			buf.WriteString("/")
		}
		buf.WriteString(gvr.Resource)
	}
	buf.WriteString("/")
	buf.WriteString(meta.GetName())
	return buf.String()
}

func formatter(summarycache *summarycache.SummaryCache, asl accesscontrol.AccessSetLookup) types.Formatter {
	return func(request *types.APIRequest, resource *types.RawResource) {
		if resource.Schema == nil {
			return
		}

		gvr := attributes.GVR(resource.Schema)
		if gvr.Version == "" {
			return
		}

		meta, err := meta.Accessor(resource.APIObject.Object)
		if err != nil {
			return
		}
		userInfo, ok := request.GetUserInfo()
		if !ok {
			return
		}
		accessSet := asl.AccessFor(userInfo)
		if accessSet == nil {
			return
		}
		hasUpdate := accessSet.Grants("update", gvr.GroupResource(), resource.APIObject.Namespace(), resource.APIObject.Name())
		hasDelete := accessSet.Grants("delete", gvr.GroupResource(), resource.APIObject.Namespace(), resource.APIObject.Name())

		selfLink := selfLink(gvr, meta)

		u := request.URLBuilder.RelativeToRoot(selfLink)
		resource.Links["view"] = u

		if hasUpdate {
			if attributes.DisallowMethods(resource.Schema)[http.MethodPut] {
				resource.Links["update"] = "blocked"
			} else {
				resource.Links["update"] = u
			}
		} else {
			delete(resource.Links, "update")
		}
		if hasDelete {
			if attributes.DisallowMethods(resource.Schema)[http.MethodDelete] {
				resource.Links["remove"] = "blocked"
			} else {
				resource.Links["remove"] = u
			}
		} else {
			delete(resource.Links, "remove")
		}

		if unstr, ok := resource.APIObject.Object.(*unstructured.Unstructured); ok {
			s, rel := summarycache.SummaryAndRelationship(unstr)
			data.PutValue(unstr.Object, map[string]interface{}{
				"name":          s.State,
				"error":         s.Error,
				"transitioning": s.Transitioning,
				"message":       strings.Join(s.Message, ":"),
			}, "metadata", "state")
			data.PutValue(unstr.Object, rel, "metadata", "relationships")

			summary.NormalizeConditions(unstr)

			includeFields(request, unstr)
			excludeFields(request, unstr)
			excludeValues(request, unstr)
		}

	}
}

func includeFields(request *types.APIRequest, unstr *unstructured.Unstructured) {
	if fields, ok := request.Query["include"]; ok {
		newObj := map[string]interface{}{}
		for _, f := range fields {
			fieldParts := strings.Split(f, ".")
			if val, ok := data.GetValue(unstr.Object, fieldParts...); ok {
				data.PutValue(newObj, val, fieldParts...)
			}
		}
		unstr.Object = newObj
	}
}

func excludeFields(request *types.APIRequest, unstr *unstructured.Unstructured) {
	if fields, ok := request.Query["exclude"]; ok {
		for _, f := range fields {
			fieldParts := strings.Split(f, ".")
			data.RemoveValue(unstr.Object, fieldParts...)
		}
	}
}

func excludeValues(request *types.APIRequest, unstr *unstructured.Unstructured) {
	if values, ok := request.Query["excludeValues"]; ok {
		for _, f := range values {
			fieldParts := strings.Split(f, ".")
			fieldValues := data.GetValueN(unstr.Object, fieldParts...)
			if obj, ok := fieldValues.(map[string]interface{}); ok {
				for k := range obj {
					data.PutValue(unstr.Object, "", append(fieldParts, k)...)
				}
			}
		}
	}
}
