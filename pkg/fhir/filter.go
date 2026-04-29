package fhir

import (
	"encoding/json"
	"lab-filter/pkg/config"
	"os"

	"github.com/samply/golang-fhir-models/fhir-models/fhir"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
)

type LabFilter struct {
	Config config.Fhir
}

type ResourceTypeDto struct {
	Type *string `bson:"resourceType" json:"resourceType"`
}

func NewLabFilter(config config.Fhir) *LabFilter {
	return &LabFilter{Config: config}
}

func (f *LabFilter) FilterBundle(fhirData []byte) *fhir.Bundle {
	bundle, err := fhir.UnmarshalBundle(fhirData)
	check(err)

	var remove []string
	var valid []fhir.BundleEntry

	var reportEntry fhir.BundleEntry
	var report fhir.DiagnosticReport

	for _, e := range bundle.Entry {
		var dto ResourceTypeDto
		err = json.Unmarshal(e.Resource, &dto)
		check(err)

		switch *dto.Type {
		case "ServiceRequest":
			if f.Config.Profiles.ServiceRequest != nil {
				request, err := fhir.UnmarshalServiceRequest(e.Resource)
				check(err)

				// set profile
				request.Meta = setProfile(request.Meta, f.Config.Profiles.ServiceRequest)

				// marshall back
				r, err := request.MarshalJSON()
				check(err)
				e.Resource = r
			}

		case "DiagnosticReport":
			reportEntry = e
			report, err = fhir.UnmarshalDiagnosticReport(e.Resource)
			check(err)

			// set profile
			report.Meta = setProfile(report.Meta, f.Config.Profiles.DiagnosticReport)

			// report will be marshaled back later
			continue

		case "Observation":
			obs, err := fhir.UnmarshalObservation(e.Resource)
			check(err)

			// set profile
			obs.Meta = setProfile(obs.Meta, f.Config.Profiles.Observation)

			if obs.ValueQuantity == nil && obs.ValueCodeableConcept == nil && obs.ValueRange == nil && obs.ValueRatio == nil {
				remove = append(remove, "Observation/"+*obs.Id)
				continue
			}

			// marshall back
			r, err := obs.MarshalJSON()
			check(err)
			e.Resource = r
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

		// return nil when bundle contains no observations
		if len(validResults) == 0 {
			log.Warning("No observations left after filter")
			return nil
		}

		report.Result = validResults
	}

	// unmarshal report
	r, err := report.MarshalJSON()
	check(err)
	reportEntry.Resource = r

	bundle.Entry = append(valid, reportEntry)

	return &bundle

}

func setProfile(meta *fhir.Meta, profile *string) *fhir.Meta {
	if profile == nil {
		return nil
	}
	if meta == nil {
		return &fhir.Meta{
			Profile: []string{*profile},
		}
	}

	meta.Profile = []string{*profile}
	return meta
}

func check(err error) {
	if err == nil {
		return
	}

	// TODO
	log.WithError(err).Error("Terminating")
	os.Exit(1)
}
