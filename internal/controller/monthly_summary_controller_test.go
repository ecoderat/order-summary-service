package controller

import (
	"errors"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"order-summary-service/internal/service"
	"order-summary-service/testdata/mockservice"
)

type MonthlySummaryControllerTestSuite struct {
	suite.Suite

	mockService *mockservice.MonthlySummaryService
	controller  MonthlySummaryController
	app         *fiber.App
}

func (suite *MonthlySummaryControllerTestSuite) SetupTest() {
	suite.mockService = &mockservice.MonthlySummaryService{}
	suite.controller = NewMonthlySummaryController(suite.mockService, nil)

	suite.app = fiber.New()
	suite.app.Get("/v1/customers/:customerId/monthly-order-summary", suite.controller.GetMonthlySummary)
}

func (suite *MonthlySummaryControllerTestSuite) TearDownTest() {
	suite.mockService.AssertExpectations(suite.T())
}

func (suite *MonthlySummaryControllerTestSuite) TestGetMonthlySummary_Success() {
	// Given: valid customer with orders
	customerID := "C123"

	now := time.Now().UTC()
	windowTo := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowFrom := windowTo.AddDate(0, 0, -30)

	expectedSummary := service.MonthlySummary{
		CustomerID: customerID,
		WindowFrom: windowFrom,
		WindowTo:   windowTo,
		OrderCount: 5,
		TotalSpend: 3890.50,
		Currency:   "TRY",
		Source:     "db",
	}

	suite.mockService.On("GetMonthlySummary", mock.Anything, customerID).
		Return(expectedSummary, nil)

	// When: request is made
	req := httptest.NewRequest("GET", "/v1/customers/"+customerID+"/monthly-order-summary", nil)
	resp, err := suite.app.Test(req)

	// Then: success response
	suite.NoError(err)
	suite.Equal(fiber.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	suite.Contains(string(body), `"customer_id":"C123"`)
	suite.Contains(string(body), `"order_count":5`)
	suite.Contains(string(body), `"total_spend":3890.5`)
	suite.Contains(string(body), `"currency":"TRY"`)
	suite.Contains(string(body), `"source":"db"`)
}

func (suite *MonthlySummaryControllerTestSuite) TestGetMonthlySummary_CacheHit() {
	// Given: cached data available
	customerID := "C456"

	now := time.Now().UTC()
	windowTo := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowFrom := windowTo.AddDate(0, 0, -30)

	expectedSummary := service.MonthlySummary{
		CustomerID: customerID,
		WindowFrom: windowFrom,
		WindowTo:   windowTo,
		OrderCount: 3,
		TotalSpend: 1500.00,
		Currency:   "USD",
		Source:     "cache",
	}

	suite.mockService.On("GetMonthlySummary", mock.Anything, customerID).
		Return(expectedSummary, nil)

	// When: request is made
	req := httptest.NewRequest("GET", "/v1/customers/"+customerID+"/monthly-order-summary", nil)
	resp, err := suite.app.Test(req)

	// Then: success with cache source
	suite.NoError(err)
	suite.Equal(fiber.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	suite.Contains(string(body), `"customer_id":"C456"`)
	suite.Contains(string(body), `"source":"cache"`)
}

func (suite *MonthlySummaryControllerTestSuite) TestGetMonthlySummary_CustomerNotFound() {
	// Given: customer doesn't exist
	customerID := "C999"

	suite.mockService.On("GetMonthlySummary", mock.Anything, customerID).
		Return(service.MonthlySummary{}, service.ErrCustomerNotFound)

	// When: request is made
	req := httptest.NewRequest("GET", "/v1/customers/"+customerID+"/monthly-order-summary", nil)
	resp, err := suite.app.Test(req)

	// Then: 404 response
	suite.NoError(err)
	suite.Equal(fiber.StatusNotFound, resp.StatusCode)
}

func (suite *MonthlySummaryControllerTestSuite) TestGetMonthlySummary_EmptyCustomerID() {
	// Given: empty customer ID in path
	req := httptest.NewRequest("GET", "/v1/customers//monthly-order-summary", nil)
	resp, err := suite.app.Test(req)

	// Then: 404 response (no service call)
	suite.NoError(err)
	suite.Equal(fiber.StatusNotFound, resp.StatusCode)
}

func (suite *MonthlySummaryControllerTestSuite) TestGetMonthlySummary_ServiceError() {
	// Given: service returns unexpected error
	customerID := "C777"

	suite.mockService.On("GetMonthlySummary", mock.Anything, customerID).
		Return(service.MonthlySummary{}, errors.New("database connection error"))

	// When: request is made
	req := httptest.NewRequest("GET", "/v1/customers/"+customerID+"/monthly-order-summary", nil)
	resp, err := suite.app.Test(req)

	// Then: 500 response
	suite.NoError(err)
	suite.Equal(fiber.StatusInternalServerError, resp.StatusCode)
}

func (suite *MonthlySummaryControllerTestSuite) TestGetMonthlySummary_EmptySummary() {
	// Given: customer exists but has no orders
	customerID := "C888"

	now := time.Now().UTC()
	windowTo := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowFrom := windowTo.AddDate(0, 0, -30)

	expectedSummary := service.MonthlySummary{
		CustomerID: customerID,
		WindowFrom: windowFrom,
		WindowTo:   windowTo,
		OrderCount: 0,
		TotalSpend: 0.0,
		Currency:   "",
		Source:     "db",
	}

	suite.mockService.On("GetMonthlySummary", mock.Anything, customerID).
		Return(expectedSummary, nil)

	// When: request is made
	req := httptest.NewRequest("GET", "/v1/customers/"+customerID+"/monthly-order-summary", nil)
	resp, err := suite.app.Test(req)

	// Then: success with zero values
	suite.NoError(err)
	suite.Equal(fiber.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	suite.Contains(string(body), `"customer_id":"C888"`)
	suite.Contains(string(body), `"order_count":0`)
	suite.Contains(string(body), `"total_spend":0`)
	suite.Contains(string(body), `"currency":""`)
}

func TestMonthlySummaryControllerTestSuite(t *testing.T) {
	suite.Run(t, new(MonthlySummaryControllerTestSuite))
}
