package parser

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cloudwego/hertz/pkg/common/errors"
	"net/http"
)

type Client struct {
	apiKey     string
	httpClient *http.Client
}

type ParseResponse struct {
	Data    string `json:"data"`
	Success bool   `json:"success"`
}

func NewClient(apiKey string) *Client {
	return &Client{
		apiKey:     apiKey,
		httpClient: http.DefaultClient}
}

func (c *Client) SetHTTPClient(httpClient *http.Client) {
	c.httpClient = httpClient
}

func (c *Client) Parse(dataset *Dataset, dsl string) (string, error) {
	dslMap := json.RawMessage{}
	err := json.Unmarshal([]byte(dsl), &dslMap)
	if err != nil {
		return "", errors.NewPublic("invalid dsl")
	}
	data := map[string]interface{}{
		"table":   dataset.name,
		"query":   dslMap,
		"meta":    dataset.generateMetaStr(),
		"dialect": dataset.dialect,
	}
	jsonData, _ := json.Marshal(data)
	postBody := bytes.NewBuffer(jsonData)
	req, err := http.NewRequest("POST", ParseUrl, postBody)
	if err != nil {
		fmt.Println("Error creating request:", err)
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(APIKeyHeader, c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return "", err
	}
	defer resp.Body.Close()

	res := ParseResponse{}
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return "", err
	}
	if !res.Success {
		return "", errors.NewPublic("parse failed")
	}
	return res.Data, nil
}
