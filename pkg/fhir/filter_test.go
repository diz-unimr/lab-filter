package fhir

import (
	"encoding/json"
	"lab-filter/pkg/config"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	//"github.com/magiconair/properties/assert"
	"github.com/samply/golang-fhir-models/fhir-models/fhir"
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
	result := NewLabFilter(config.Fhir{Profiles: config.Profiles{
		ServiceRequest:   nil,
		DiagnosticReport: nil,
		Observation:      nil,
	}}).FilterBundle(testBundle)

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

	assert.ElementsMatchf(t, actual, expected, "Expected bundle to contain entry ids (%s) but was (%s)",
		strings.Join(expected, ","), strings.Join(actual, ","))
}

func TestFilterSetsProfiles(t *testing.T) {
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
        "resourceType": "ServiceRequest",
        "id": "request",
        "meta": {
          "source": "#swisslab",
          "tag": [
            {
              "system": "https://fhir.diz.uni-marburg.de/CodeSystem/mapper",
              "code": "hl7-lab-mapper"
            }
          ]
        }
      }
    }
  ]
}
`)
	profiles := config.Fhir{Profiles: config.Profiles{
		ServiceRequest:   ref("https://www.medizininformatik-initiative.de/fhir/core/modul-labor/StructureDefinition/ServiceRequestLab|2026.0.0"),
		DiagnosticReport: ref("https://www.medizininformatik-initiative.de/fhir/core/modul-labor/StructureDefinition/DiagnosticReportLab|2026.0.0"),
		Observation:      ref("https://www.medizininformatik-initiative.de/fhir/core/modul-labor/StructureDefinition/ObservationLab|2026.0.0"),
	}}

	// act
	result := NewLabFilter(profiles).FilterBundle(testBundle)

	// assert
	for _, e := range result.Entry {
		res, _ := fhir.UnmarshalResource(e.Resource)
		var dto ResourceTypeDto
		_ = json.Unmarshal(e.Resource, &dto)
		switch *dto.Type {
		case "ServiceRequest":
			assert.Equal(t, res.Meta.Profile, []string{*profiles.Profiles.ServiceRequest})
		case "DiagnosticReport":
			assert.Equal(t, res.Meta.Profile, []string{*profiles.Profiles.DiagnosticReport})
		case "Observation":
			assert.Equal(t, res.Meta.Profile, []string{*profiles.Profiles.Observation})
		}
	}
}

func ref(str string) *string {
	return &str
}
