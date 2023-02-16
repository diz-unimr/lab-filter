package fhir

import (
	"github.com/samply/golang-fhir-models/fhir-models/fhir"
	"golang.org/x/exp/slices"
	"strings"
	"testing"
)

func TestFilterBundle(t *testing.T) {
	// test bundle
	testBundle := []byte(`
{
  "resourceType": "Bundle",
  "type": "batch",
  "entry": [
    {
      "resource": {
        "resourceType": "DiagnosticReport",
        "id": "report",
        "result": [
          {
            "reference": "Observation/quantity-obs"
          },
          {
			"reference": "Observation/str-obs"
          }
        ]
      }
    },
    {
      "resource": {
        "resourceType": "Observation",
        "id": "quantity-obs",
        "valueQuantity": {
          "value": 24,
          "unit": "µg/l",
          "system": "http://unitsofmeasure.org",
          "code": "ng/mL"
        }
      }
    },
    {
      "resource": {
        "resourceType": "Observation",
        "id": "str-obs",
        "valueString": "test"
      }
    }
  ]
}
`)

	// act
	result := FilterBundle(testBundle)

	// assert
	expected := []string{"report", "quantity-obs"}

	// not nil
	if result == nil {
		t.Errorf("Expected bundle to contains entry ids (%s) but bundle was (nil)", strings.Join(expected, ","))
		return
	}

	// assert valid entries
	entries := (*result).Entry
	var actual []string
	for _, e := range entries {
		res, _ := fhir.UnmarshalResource(e.Resource)
		actual = append(actual, *res.Id)
	}

	if !slices.Equal(actual, expected) {
		t.Errorf("Expected bundle to contain entry ids (%s) but was (%s)", strings.Join(expected, ","), strings.Join(actual, ","))
	}
}
