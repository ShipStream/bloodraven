package runner

import (
	"encoding/xml"
	"fmt"
	"os"
)

// JUnitTestSuite is the surefire-flavor JUnit XML structure.
type JUnitTestSuite struct {
	XMLName  xml.Name        `xml:"testsuite"`
	Name     string          `xml:"name,attr"`
	Tests    int             `xml:"tests,attr"`
	Failures int             `xml:"failures,attr"`
	Errors   int             `xml:"errors,attr"`
	Time     float64         `xml:"time,attr"`
	Cases    []JUnitTestCase `xml:"testcase"`
}

// JUnitTestCase is one scenario.
type JUnitTestCase struct {
	XMLName   xml.Name `xml:"testcase"`
	Name      string   `xml:"name,attr"`
	Classname string   `xml:"classname,attr"`
	Time      float64  `xml:"time,attr"`
	Failure   *junitFailure `xml:"failure,omitempty"`
}

type junitFailure struct {
	Type    string `xml:"type,attr"`
	Message string `xml:"message,attr"`
	Body    string `xml:",chardata"`
}

// WriteJUnit writes a JUnit XML file describing the run-all results.
func WriteJUnit(path string, results []Result) error {
	if path == "" {
		return nil
	}
	suite := JUnitTestSuite{Name: "playground-chaos"}
	for _, r := range results {
		suite.Tests++
		suite.Time += r.Duration.Seconds()
		tc := JUnitTestCase{
			Name:      r.ID,
			Classname: "playground-chaos",
			Time:      r.Duration.Seconds(),
		}
		if !r.Passed {
			suite.Failures++
			tc.Failure = &junitFailure{
				Type: fmt.Sprintf("%s/%s", r.Phase, r.StepName),
				Message: r.Failure,
				Body: fmt.Sprintf("phase=%s step=%q\n%s\n\nForensics: %s",
					r.Phase, r.StepName, r.Failure, r.CapturePath),
			}
		}
		suite.Cases = append(suite.Cases, tc)
	}

	body, err := xml.MarshalIndent(suite, "", "  ")
	if err != nil {
		return err
	}
	body = append([]byte(xml.Header), body...)
	body = append(body, '\n')

	if err := os.WriteFile(path, body, 0o644); err != nil {
		return err
	}
	return nil
}
