package fhir

import (
	"encoding/json"
	"os"

	"github.com/samply/golang-fhir-models/fhir-models/fhir"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

type ResourceTypeDto struct {
	Type *string `bson:"resourceType" json:"resourceType"`
}

func FilterBundle(fhirData []byte) *fhir.Bundle {
	bundle, err := fhir.UnmarshalBundle(fhirData)
	check(err)

	var remove []string
	var valid []fhir.BundleEntry

	var report fhir.DiagnosticReport

	for _, e := range bundle.Entry {
		var dto ResourceTypeDto
		err = json.Unmarshal(e.Resource, &dto)
		check(err)

		if *dto.Type == "DiagnosticReport" {
			report, err = fhir.UnmarshalDiagnosticReport(e.Resource)
			check(err)
		}

		if *dto.Type == "Observation" {
			obs, err := fhir.UnmarshalObservation(e.Resource)
			check(err)

			if !(obs.ValueQuantity != nil || obs.ValueCodeableConcept != nil || obs.ValueRange != nil || obs.ValueRatio != nil) {
				remove = append(remove, "Observation/"+*obs.Id)
				continue
			}
		}

		valid = append(valid, e)
	}

	var validResults []fhir.Reference
	if len(remove) > 0 {
		for _, r := range report.Result {
			if !slices.Contains(remove, *r.Reference) {
				validResults = append(validResults, r)
			}
		}

		// return err when bundle contains no observations
		if len(validResults) == 0 {
			log.Warning("no observations left after filter")
			return nil
		}
		report.Result = validResults
	}
	bundle.Entry = valid

	return &bundle

}

func check(err error) {
	if err == nil {
		return
	}

	// TODO
	log.WithError(err).Error("Terminating")
	os.Exit(1)
}
