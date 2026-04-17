package service

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"order-summary-service/internal/cache"
	"order-summary-service/internal/repository"
	"order-summary-service/testdata/mockcache"
	"order-summary-service/testdata/mockrepository"
)

type MonthlySummaryServiceTestSuite struct {
	suite.Suite

	mockRepo  mockrepository.Repository
	mockCache *mockcache.Cache

	service MonthlySummaryService
}

func (suite *MonthlySummaryServiceTestSuite) SetupTest() {
	suite.mockRepo = mockrepository.Repository{}
	suite.mockCache = &mockcache.Cache{}

	suite.service = NewMonthlySummaryService(&suite.mockRepo, suite.mockCache, nil, 0, 0, 0)
}

func (suite *MonthlySummaryServiceTestSuite) TearDownTest() {
	suite.mockRepo.AssertExpectations(suite.T())
	suite.mockCache.AssertExpectations(suite.T())
}

func (suite *MonthlySummaryServiceTestSuite) TestGetMonthlySummary_Success() {
	ctx := context.Background()
	customerID := "C123"

	now := time.Now().UTC()
	windowTo := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowFrom := windowTo.AddDate(0, 0, -30)
	dateKey := cache.MonthlyDateString(windowTo)

	expectedSummary := repository.MonthlySummary{
		CustomerID: customerID,
		WindowFrom: windowFrom,
		WindowTo:   windowTo,
		OrderCount: 5,
		TotalSpend: decimal.NewFromFloat(3890.50),
		Currency:   "TRY",
	}

	// Mock cache operations
	suite.mockCache.On("MarkMonthlyHot", mock.Anything, customerID, dateKey, mock.Anything).Return(nil)
	suite.mockCache.On("CacheGet", mock.Anything, customerID, dateKey).Return("", false, nil)
	suite.mockCache.On("AcquireMonthlyLock", mock.Anything, customerID, dateKey, mock.Anything).Return("test-token", true, nil)
	suite.mockCache.On("CacheSet", mock.Anything, customerID, dateKey, mock.Anything, mock.Anything).Return(nil)
	suite.mockCache.On("ReleaseMonthlyLock", mock.Anything, customerID, dateKey, mock.Anything).Return(nil)

	// Mock repository operations
	suite.mockRepo.On("CustomerExistsFinal", ctx, customerID).Return(true, nil)
	suite.mockRepo.On("MonthlySummaryFinal", ctx, customerID, windowFrom, windowTo).
		Return(expectedSummary, nil)

	// Execute
	result, err := suite.service.GetMonthlySummary(ctx, customerID)

	// Assert
	suite.NoError(err)
	suite.Equal(customerID, result.CustomerID)
	suite.Equal(windowFrom, result.WindowFrom)
	suite.Equal(windowTo, result.WindowTo)
	suite.Equal(uint64(5), result.OrderCount)
	suite.Equal(3890.50, result.TotalSpend)
	suite.Equal("TRY", result.Currency)
	suite.Equal("db", result.Source)
}

func (suite *MonthlySummaryServiceTestSuite) TestGetMonthlySummary_CacheHit() {
	// Given: cache hit with valid payload
	ctx := context.Background()
	customerID := "C456"

	now := time.Now().UTC()
	windowTo := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowFrom := windowTo.AddDate(0, 0, -30)
	dateKey := cache.MonthlyDateString(windowTo)

	// Prepare cached data
	cachedEntry := cache.MonthlySummaryEntry{
		CustomerID: customerID,
		WindowFrom: windowFrom,
		WindowTo:   windowTo,
		OrderCount: 3,
		TotalSpend: 1500.00,
		Currency:   "USD",
	}
	payload, _ := cache.EncodeMonthlySummary(cachedEntry)

	// Mock: hot marker succeeds
	suite.mockCache.On("MarkMonthlyHot", mock.Anything, customerID, dateKey, mock.Anything).Return(nil)
	// Mock: cache hit
	suite.mockCache.On("CacheGet", mock.Anything, customerID, dateKey).Return(payload, true, nil)

	// When
	result, err := suite.service.GetMonthlySummary(ctx, customerID)

	// Then: cache hit, no repository calls
	suite.NoError(err)
	suite.Equal(customerID, result.CustomerID)
	suite.Equal(windowFrom, result.WindowFrom)
	suite.Equal(windowTo, result.WindowTo)
	suite.Equal(uint64(3), result.OrderCount)
	suite.Equal(1500.00, result.TotalSpend)
	suite.Equal("USD", result.Currency)
	suite.Equal("cache", result.Source)
}

func (suite *MonthlySummaryServiceTestSuite) TestGetMonthlySummary_CustomerNotFound() {
	// Given: cache miss, customer doesn't exist
	ctx := context.Background()
	customerID := "C999"

	now := time.Now().UTC()
	windowTo := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	dateKey := cache.MonthlyDateString(windowTo)

	// Mock cache operations
	suite.mockCache.On("MarkMonthlyHot", mock.Anything, customerID, dateKey, mock.Anything).Return(nil)
	suite.mockCache.On("CacheGet", mock.Anything, customerID, dateKey).Return("", false, nil)
	suite.mockCache.On("AcquireMonthlyLock", mock.Anything, customerID, dateKey, mock.Anything).Return("test-token", true, nil)
	suite.mockCache.On("ReleaseMonthlyLock", mock.Anything, customerID, dateKey, mock.Anything).Return(nil)

	// Mock: customer doesn't exist
	suite.mockRepo.On("CustomerExistsFinal", ctx, customerID).Return(false, nil)

	// When
	result, err := suite.service.GetMonthlySummary(ctx, customerID)

	// Then: customer not found error
	suite.Error(err)
	suite.Equal(ErrCustomerNotFound, err)
	suite.Empty(result.CustomerID)
}

func (suite *MonthlySummaryServiceTestSuite) TestGetMonthlySummary_EmptySummaryWhenNotFoundInDB() {
	// Given: cache miss, customer exists, but no orders in window
	ctx := context.Background()
	customerID := "C789"

	now := time.Now().UTC()
	windowTo := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowFrom := windowTo.AddDate(0, 0, -30)
	dateKey := cache.MonthlyDateString(windowTo)

	// Mock cache operations
	suite.mockCache.On("MarkMonthlyHot", mock.Anything, customerID, dateKey, mock.Anything).Return(nil)
	suite.mockCache.On("CacheGet", mock.Anything, customerID, dateKey).Return("", false, nil)
	suite.mockCache.On("AcquireMonthlyLock", mock.Anything, customerID, dateKey, mock.Anything).Return("test-token", true, nil)
	suite.mockCache.On("CacheSet", mock.Anything, customerID, dateKey, mock.Anything, mock.Anything).Return(nil)
	suite.mockCache.On("ReleaseMonthlyLock", mock.Anything, customerID, dateKey, mock.Anything).Return(nil)

	// Mock: customer exists
	suite.mockRepo.On("CustomerExistsFinal", ctx, customerID).Return(true, nil)
	// Mock: no orders found
	suite.mockRepo.On("MonthlySummaryFinal", ctx, customerID, windowFrom, windowTo).
		Return(repository.MonthlySummary{}, repository.ErrMonthlySummaryNotFound)

	// When
	result, err := suite.service.GetMonthlySummary(ctx, customerID)

	// Then: empty summary returned
	suite.NoError(err)
	suite.Equal(customerID, result.CustomerID)
	suite.Equal(windowFrom, result.WindowFrom)
	suite.Equal(windowTo, result.WindowTo)
	suite.Equal(uint64(0), result.OrderCount)
	suite.Equal(0.0, result.TotalSpend)
	suite.Equal("", result.Currency)
	suite.Equal("db", result.Source)
}

func (suite *MonthlySummaryServiceTestSuite) TestGetMonthlySummary_LockNotAcquired_RetryCacheHit() {
	// Given: cache miss initially, lock not acquired, retry finds cache hit
	ctx := context.Background()
	customerID := "C555"

	now := time.Now().UTC()
	windowTo := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowFrom := windowTo.AddDate(0, 0, -30)
	dateKey := cache.MonthlyDateString(windowTo)

	// Prepare cached data for retry
	cachedEntry := cache.MonthlySummaryEntry{
		CustomerID: customerID,
		WindowFrom: windowFrom,
		WindowTo:   windowTo,
		OrderCount: 7,
		TotalSpend: 2500.75,
		Currency:   "EUR",
	}
	payload, _ := cache.EncodeMonthlySummary(cachedEntry)

	// Mock cache operations
	suite.mockCache.On("MarkMonthlyHot", mock.Anything, customerID, dateKey, mock.Anything).Return(nil)
	// First cache get: miss
	suite.mockCache.On("CacheGet", mock.Anything, customerID, dateKey).Return("", false, nil).Once()
	// Lock acquisition fails — another goroutine holds it
	suite.mockCache.On("AcquireMonthlyLock", mock.Anything, customerID, dateKey, mock.Anything).Return("", false, nil)
	// Retry cache get: hit
	suite.mockCache.On("CacheGet", mock.Anything, customerID, dateKey).Return(payload, true, nil).Once()

	// When
	result, err := suite.service.GetMonthlySummary(ctx, customerID)

	// Then: cache hit on retry, no repository calls
	suite.NoError(err)
	suite.Equal(customerID, result.CustomerID)
	suite.Equal(windowFrom, result.WindowFrom)
	suite.Equal(windowTo, result.WindowTo)
	suite.Equal(uint64(7), result.OrderCount)
	suite.Equal(2500.75, result.TotalSpend)
	suite.Equal("EUR", result.Currency)
	suite.Equal("cache", result.Source)
}

func (suite *MonthlySummaryServiceTestSuite) TestGetMonthlySummary_LockNotAcquired_AllRetriesMiss_FallsThroughToDB() {
	// Given: cache miss, lock held by another goroutine, all retries miss,
	// so this goroutine falls through and queries the DB directly.
	// Crucially: it must NOT write the result to cache (it is not the lock holder).
	ctx := context.Background()
	customerID := "C111"

	now := time.Now().UTC()
	windowTo := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowFrom := windowTo.AddDate(0, 0, -30)
	dateKey := cache.MonthlyDateString(windowTo)

	expectedSummary := repository.MonthlySummary{
		CustomerID: customerID,
		WindowFrom: windowFrom,
		WindowTo:   windowTo,
		OrderCount: 3,
		TotalSpend: decimal.NewFromFloat(750.00),
		Currency:   "TRY",
	}

	suite.mockCache.On("MarkMonthlyHot", mock.Anything, customerID, dateKey, mock.Anything).Return(nil)
	// Initial cache check: miss.
	suite.mockCache.On("CacheGet", mock.Anything, customerID, dateKey).Return("", false, nil).Once()
	// Lock not acquired — another goroutine holds it.
	suite.mockCache.On("AcquireMonthlyLock", mock.Anything, customerID, dateKey, mock.Anything).Return("", false, nil)
	// All retries inside waitForCacheOrGiveUp also miss.
	suite.mockCache.On("CacheGet", mock.Anything, customerID, dateKey).Return("", false, nil).Times(defaultLockRetry)

	// Fallthrough to DB.
	suite.mockRepo.On("CustomerExistsFinal", ctx, customerID).Return(true, nil).Once()
	suite.mockRepo.On("MonthlySummaryFinal", ctx, customerID, windowFrom, windowTo).
		Return(expectedSummary, nil).Once()

	// When
	result, err := suite.service.GetMonthlySummary(ctx, customerID)

	// Then: DB result is returned correctly.
	suite.NoError(err)
	suite.Equal(customerID, result.CustomerID)
	suite.Equal(windowFrom, result.WindowFrom)
	suite.Equal(windowTo, result.WindowTo)
	suite.Equal(uint64(3), result.OrderCount)
	suite.Equal(750.00, result.TotalSpend)
	suite.Equal("TRY", result.Currency)
	suite.Equal("db", result.Source)
	// Verify the full retry loop executed: 1 initial check + defaultLockRetry retries.
	suite.mockCache.AssertNumberOfCalls(suite.T(), "CacheGet", 1+defaultLockRetry)
	suite.mockRepo.AssertNumberOfCalls(suite.T(), "CustomerExistsFinal", 1)
	suite.mockRepo.AssertNumberOfCalls(suite.T(), "MonthlySummaryFinal", 1)
	// Loser must not write to cache or release a lock it does not hold.
	suite.mockCache.AssertNotCalled(suite.T(), "CacheSet")
	suite.mockCache.AssertNotCalled(suite.T(), "ReleaseMonthlyLock")
}

func TestMonthlySummaryServiceTestSuite(t *testing.T) {
	suite.Run(t, new(MonthlySummaryServiceTestSuite))
}
