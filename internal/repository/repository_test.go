package repository

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"order-summary-service/testdata/mockclickhousebatch"
	"order-summary-service/testdata/mockclickhouseconnection"
	"order-summary-service/testdata/mockclickhouserow"
)

type RepositoryTestSuite struct {
	suite.Suite

	mockConn  *mockclickhouseconnection.Connection
	mockBatch *mockclickhousebatch.Batch
	repo      Repository
}

func TestRepositoryTestSuite(t *testing.T) {
	suite.Run(t, new(RepositoryTestSuite))
}

func (s *RepositoryTestSuite) SetupTest() {
	s.mockConn = &mockclickhouseconnection.Connection{}
	s.mockBatch = &mockclickhousebatch.Batch{}
	s.repo = &repository{conn: s.mockConn}
}

func (s *RepositoryTestSuite) TearDownTest() {
	s.mockConn.AssertExpectations(s.T())
	s.mockBatch.AssertExpectations(s.T())
}

func (s *RepositoryTestSuite) TestMonthlySummaryFinal_Success() {
	var (
		customerID = "customer-123"
		windowFrom = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		windowTo   = time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

		expected = MonthlySummary{
			CustomerID: customerID,
			WindowFrom: windowFrom,
			WindowTo:   windowTo,
			OrderCount: 10,
			TotalSpend: decimal.NewFromFloat(1500.75),
			Currency:   "USD",
		}
	)

	row := newMonthlySummaryRow(expected)

	s.mockConn.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(row).Once()

	got, err := s.repo.MonthlySummaryFinal(context.Background(), customerID, windowFrom, windowTo)
	s.NoError(err)
	s.Equal(expected, got)
}

func (s *RepositoryTestSuite) TestMonthlySummaryFinal_NotFound() {
	var (
		customerID = "customer-123"
		windowFrom = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		windowTo   = time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	)

	row := mockclickhouserow.Row{
		ScanFn: func(dest ...any) error {
			return sql.ErrNoRows
		},
	}

	s.mockConn.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(row).Once()

	_, err := s.repo.MonthlySummaryFinal(context.Background(), customerID, windowFrom, windowTo)
	s.ErrorIs(err, ErrMonthlySummaryNotFound)
}

func (s *RepositoryTestSuite) TestMonthlySummaryFinal_FailedScan() {
	var (
		customerID = "customer-123"
		windowFrom = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		windowTo   = time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
	)

	row := mockclickhouserow.Row{
		ScanFn: func(dest ...any) error {
			return sql.ErrConnDone
		},
	}

	s.mockConn.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(row).Once()

	_, err := s.repo.MonthlySummaryFinal(context.Background(), customerID, windowFrom, windowTo)
	s.ErrorIs(err, sql.ErrConnDone)
}

func (s *RepositoryTestSuite) TestCustomerExistsFinal_Exists() {
	var customerID = "customer-123"

	row := mockclickhouserow.Row{
		ScanFn: func(dest ...any) error {
			*(dest[0].(*uint8)) = 1
			return nil
		},
	}

	s.mockConn.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(row).Once()

	exists, err := s.repo.CustomerExistsFinal(context.Background(), customerID)
	s.NoError(err)
	s.True(exists)
}

func (s *RepositoryTestSuite) TestCustomerExistsFinal_NotExists() {
	var customerID = "customer-123"

	row := mockclickhouserow.Row{
		ScanFn: func(dest ...any) error {
			return sql.ErrNoRows
		},
	}

	s.mockConn.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(row).Once()

	exists, err := s.repo.CustomerExistsFinal(context.Background(), customerID)
	s.NoError(err)
	s.False(exists)
}

func (s *RepositoryTestSuite) TestCustomerExistsFinal_FailedScan() {
	var customerID = "customer-123"

	row := mockclickhouserow.Row{
		ScanFn: func(dest ...any) error {
			return sql.ErrConnDone
		},
	}

	s.mockConn.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(row).Once()

	exists, err := s.repo.CustomerExistsFinal(context.Background(), customerID)
	s.ErrorIs(err, sql.ErrConnDone)
	s.False(exists)
}

func (s *RepositoryTestSuite) TestCreateCustomerBatch_Success() {
	customers := []CreateCustomerParams{
		{
			CustomerID:    "customer-1",
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
			SourceEventID: uuid.NewString(),
		},
		{
			CustomerID:    "customer-2",
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
			SourceEventID: uuid.NewString(),
		},
	}

	s.mockConn.On("PrepareBatch", mock.Anything, mock.Anything).Return(s.mockBatch, nil).Once()

	s.mockBatch.On("Append", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()

	s.mockBatch.On("Send").Return(nil).Once()

	err := s.repo.CreateCustomerBatch(context.Background(), customers)
	s.NoError(err)
}

func (s *RepositoryTestSuite) TestCreateCustomerBatch_PrepareBatchError() {
	customers := []CreateCustomerParams{
		{
			CustomerID:    "customer-1",
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
			SourceEventID: uuid.NewString(),
		},
	}

	s.mockConn.On("PrepareBatch", mock.Anything, mock.Anything).Return(nil, sql.ErrConnDone).Once()

	err := s.repo.CreateCustomerBatch(context.Background(), customers)
	s.ErrorIs(err, sql.ErrConnDone)
}

func (s *RepositoryTestSuite) TestCreateCustomerBatch_AppendError() {
	customers := []CreateCustomerParams{
		{
			CustomerID:    "customer-1",
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
			SourceEventID: "invalid-uuid",
		},
	}

	s.mockConn.On("PrepareBatch", mock.Anything, mock.Anything).Return(s.mockBatch, nil).Once()

	err := s.repo.CreateCustomerBatch(context.Background(), customers)
	s.Error(err)
}

func (s *RepositoryTestSuite) TestCreateCustomerBatch_SendError() {
	customers := []CreateCustomerParams{
		{
			CustomerID:    "customer-1",
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
			SourceEventID: uuid.NewString(),
		},
	}

	s.mockConn.On("PrepareBatch", mock.Anything, mock.Anything).Return(s.mockBatch, nil).Once()

	s.mockBatch.On("Append", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	s.mockBatch.On("Send").Return(sql.ErrConnDone).Once()

	err := s.repo.CreateCustomerBatch(context.Background(), customers)
	s.ErrorIs(err, sql.ErrConnDone)
}

func (s *RepositoryTestSuite) TestCreateOrderBatch_Success() {
	orders := []CreateOrderParams{
		{
			OrderID:       "order-1",
			CustomerID:    "customer-1",
			OrderTime:     time.Now(),
			TotalAmount:   100.50,
			Currency:      "USD",
			SourceEventID: uuid.NewString(),
		},
		{
			OrderID:       "order-2",
			CustomerID:    "customer-2",
			OrderTime:     time.Now(),
			TotalAmount:   200.75,
			Currency:      "USD",
			SourceEventID: uuid.NewString(),
		},
	}

	s.mockConn.On("PrepareBatch", mock.Anything, mock.Anything).Return(s.mockBatch, nil).Once()

	s.mockBatch.On("Append", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()

	s.mockBatch.On("Send").Return(nil).Once()

	err := s.repo.CreateOrderBatch(context.Background(), orders)
	s.NoError(err)
}

func (s *RepositoryTestSuite) TestCreateOrderBatch_PrepareBatchError() {
	orders := []CreateOrderParams{
		{
			OrderID:       "order-1",
			CustomerID:    "customer-1",
			OrderTime:     time.Now(),
			TotalAmount:   100.50,
			Currency:      "USD",
			SourceEventID: uuid.NewString(),
		},
	}

	s.mockConn.On("PrepareBatch", mock.Anything, mock.Anything).Return(nil, sql.ErrConnDone).Once()

	err := s.repo.CreateOrderBatch(context.Background(), orders)
	s.ErrorIs(err, sql.ErrConnDone)
}

func (s *RepositoryTestSuite) TestCreateOrderBatch_AppendError() {
	orders := []CreateOrderParams{
		{
			OrderID:       "order-1",
			CustomerID:    "customer-1",
			OrderTime:     time.Now(),
			TotalAmount:   100.50,
			Currency:      "USD",
			SourceEventID: "invalid-uuid",
		},
	}

	s.mockConn.On("PrepareBatch", mock.Anything, mock.Anything).Return(s.mockBatch, nil).Once()

	err := s.repo.CreateOrderBatch(context.Background(), orders)
	s.Error(err)
}

func (s *RepositoryTestSuite) TestCreateOrderBatch_SendError() {
	orders := []CreateOrderParams{
		{
			OrderID:       "order-1",
			CustomerID:    "customer-1",
			OrderTime:     time.Now(),
			TotalAmount:   100.50,
			Currency:      "USD",
			SourceEventID: uuid.NewString(),
		},
	}

	s.mockConn.On("PrepareBatch", mock.Anything, mock.Anything).Return(s.mockBatch, nil).Once()

	s.mockBatch.On("Append", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	s.mockBatch.On("Send").Return(sql.ErrConnDone).Once()

	err := s.repo.CreateOrderBatch(context.Background(), orders)
	s.ErrorIs(err, sql.ErrConnDone)
}

func newMonthlySummaryRow(expected MonthlySummary) mockclickhouserow.Row {
	return mockclickhouserow.Row{
		ScanFn: func(dest ...any) error {
			*(dest[0].(*string)) = expected.CustomerID
			*(dest[1].(*time.Time)) = expected.WindowFrom
			*(dest[2].(*time.Time)) = expected.WindowTo
			*(dest[3].(*uint64)) = expected.OrderCount
			*(dest[4].(*decimal.Decimal)) = expected.TotalSpend
			*(dest[5].(*string)) = expected.Currency
			return nil
		},
	}
}
