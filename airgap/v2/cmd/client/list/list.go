// Copyright 2021 IBM Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package list

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/redhat-marketplace/redhat-marketplace-operator/airgap/v2/apis/fileretreiver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Listconfig struct {
	filter []string
	sort   []string
	conn   *grpc.ClientConn
	client fileretreiver.FileRetreiverClient
}

var (
	lc  Listconfig
	log logr.Logger
)

var ListCmd = &cobra.Command{
	Use:   "list",
	Short: "List files ",
	Long: `
	Allowed comparison operators: EQUAL, CONTAINS, LESS_THAN, GREATER_THAN
	Allowed sort operators: ASC, DESC
	-----------------------------------------------------------------------
	Pre-defined search keys: 
	[provided_id] refers to the file identifier
	[provided_name] refers to the name of the file
	[size] refer to the size of file
	[created_at] refers to file creation date (expected format yyyy-mm-dd)
	[deleted_at] refer to the file deletion date  (expected format yyyy-mm-dd)
	-----------------------------------------------------------------------
	Pre-defined sort keys: 
	[provided_id] refers to the file identifier
	[provided_name] refers to the name of the file
	[size] refer to the size of file
	[created_at] refers to file creation date (expected format yyyy-mm-dd)
	[deleted_at] refer to the file deletion date  (expected format yyyy-mm-dd)
	`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize client
		err := lc.initializeListClient()
		if err != nil {
			return err
		}
		defer lc.closeConnection()
		return lc.listFileMetadata()
	},
}

func init() {
	initLog()
	ListCmd.Flags().StringSliceVarP(&lc.filter, "filter", "f", []string{}, "List file using date")
	ListCmd.Flags().StringSliceVarP(&lc.sort, "sort", "s", []string{}, "List file using date")
}

func initLog() {
	zapLog, err := zap.NewDevelopment()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize zapr, due to error: %v", err))
	}
	log = zapr.NewLogger(zapLog)
}

func (lc *Listconfig) initializeListClient() error {
	// Fetch target address
	address := viper.GetString("address")
	if len(strings.TrimSpace(address)) == 0 {
		return fmt.Errorf("target address is blank/empty")
	}
	log.Info(fmt.Sprintf("Target address: %v", address))

	// Create connection
	insecure := viper.GetBool("insecure")
	var conn *grpc.ClientConn
	var err error

	if insecure {
		conn, err = grpc.Dial(address, grpc.WithInsecure())
	} else {
		cert := viper.GetString("certificate-path")
		creds, sslErr := credentials.NewClientTLSFromFile(cert, "")
		if sslErr != nil {
			return fmt.Errorf("ssl error: %v", sslErr)
		}
		opts := grpc.WithTransportCredentials(creds)
		conn, err = grpc.Dial(address, opts)
	}

	// Handle any connection errors
	if err != nil {
		return fmt.Errorf("connection error: %v", err)
	}

	lc.client = fileretreiver.NewFileRetreiverClient(conn)
	lc.conn = conn
	return nil
}

// closeConnection closes the grpc client connection
func (lc *Listconfig) closeConnection() {
	if lc != nil && lc.conn != nil {
		lc.conn.Close()
	}
}

// listFileMetadata fetch list of files and its metadata from the grpc server to a specified directory
func (lc *Listconfig) listFileMetadata() error {

	var filter_list []*fileretreiver.ListFileMetadataRequest_ListFileFilter
	var sort_list []*fileretreiver.ListFileMetadataRequest_ListFileSort

	filter_list, err := parseFilter(lc.filter)
	if err != nil {
		return err
	}

	sort_list, err = parseSort(lc.sort)
	if err != nil {
		return err
	}

	req := &fileretreiver.ListFileMetadataRequest{
		FilterBy: filter_list,
		SortBy:   sort_list,
	}

	resultStream, err := lc.client.ListFileMetadata(context.Background(), req)
	if err != nil {
		return fmt.Errorf("failed to Retrive list due to: %v", err)
	}

	log.Info("-----------------OUTPUT-----------------")
	for {
		response, err := resultStream.Recv()
		if err == io.EOF {
			log.Info("Server stream end")
			break
		} else if err != nil {
			return fmt.Errorf("error while reading stream: %v", err)
		}

		data := response.GetResults()
		fmt.Println("Data: ", data)
	}

	return nil
}

// parseFilter parses arguments for filter operation
// It returns list of struct ListFileMetadataRequest_ListFileFilter and error if occured
func parseFilter(filter []string) ([]*fileretreiver.ListFileMetadataRequest_ListFileFilter, error) {

	modelColumnSet := map[string]bool{
		"provided_name": true,
		"provided_id":   true,
		"size":          true,
		"created_at":    true,
		"deleted_at":    true,
	}

	var list_filter []*fileretreiver.ListFileMetadataRequest_ListFileFilter
	for _, filter_string := range filter {

		args := strings.Split(filter_string, " ")
		var filter_args []string
		for _, arg := range args {
			if len(strings.TrimSpace(arg)) != 0 {
				filter_args = append(filter_args, arg)
			}
		}
		if len(filter_args) != 3 {
			return nil,
				fmt.Errorf("'%v' : Invalid number of arguments provided for filter operation, Required 3 | Provided %v ",
					filter_string, len(filter_args))
		}

		operator, err := parseFilterOperator(filter_args[1], modelColumnSet[filter_args[0]])
		if err != nil {
			return nil, err
		}

		var value string
		if filter_args[0] == "created_at" || filter_args[0] == "deleted_at" {
			value, err = parseDateToEpoch(filter_args[2])
			if err != nil {
				return nil, err
			}
		} else {
			value = filter_args[2]
		}

		req := &fileretreiver.ListFileMetadataRequest_ListFileFilter{
			Key:      filter_args[0],
			Operator: *operator,
			Value:    value,
		}
		list_filter = append(list_filter, req)
	}
	return list_filter, nil
}

// parseFilterOperator parses filter operator and returns respective operator defined by fileretreiver.proto
func parseFilterOperator(op string, isColumn bool) (*fileretreiver.ListFileMetadataRequest_ListFileFilter_Comparison, error) {
	var operator fileretreiver.ListFileMetadataRequest_ListFileFilter_Comparison
	if isColumn {
		switch op {
		case "EQUAL":
			operator = fileretreiver.ListFileMetadataRequest_ListFileFilter_EQUAL
		case "LESS_THAN":
			operator = fileretreiver.ListFileMetadataRequest_ListFileFilter_LESS_THAN
		case "GREATER_THAN":
			operator = fileretreiver.ListFileMetadataRequest_ListFileFilter_GREATER_THAN
		case "CONTAINS":
			operator = fileretreiver.ListFileMetadataRequest_ListFileFilter_CONTAINS
		default:
			return nil, fmt.Errorf("Invalid Filter Operation Used")
		}
	} else {
		switch op {
		case "EQUAL":
			operator = fileretreiver.ListFileMetadataRequest_ListFileFilter_EQUAL
		case "CONTAINS":
			operator = fileretreiver.ListFileMetadataRequest_ListFileFilter_CONTAINS
		default:
			return nil, fmt.Errorf("Invalid Filter Operation Used")
		}
	}

	return &operator, nil
}

// parseSort parses arguments for sort operation
// It returns list of struct ListFileMetadataRequest_ListFileSort and error id occured
func parseSort(sort_list []string) ([]*fileretreiver.ListFileMetadataRequest_ListFileSort, error) {
	modelColumnSet := map[string]bool{
		"provided_name": true,
		"provided_id":   true,
		"size":          true,
		"created_at":    true,
		"deleted_at":    true,
	}
	var list_sort []*fileretreiver.ListFileMetadataRequest_ListFileSort

	for _, sort_string := range sort_list {
		args := strings.Split(sort_string, " ")
		var sort_args []string
		for _, arg := range args {
			if len(strings.TrimSpace(arg)) != 0 {
				sort_args = append(sort_args, arg)
			}
		}
		if len(sort_args) != 2 {
			return nil, fmt.Errorf("'%v' : Invalid number of arguments provided for sort operation, Required 2 | Provided %v ", sort_string, len(sort_args))
		}
		if !modelColumnSet[sort_args[0]] {
			return nil, fmt.Errorf("Invalid Operand passed for sort operation.")
		}
		operator, err := parseSortOperator(sort_args[1])
		if err != nil {
			return nil, err
		}
		req := &fileretreiver.ListFileMetadataRequest_ListFileSort{
			Key:       sort_args[0],
			SortOrder: *operator,
		}
		list_sort = append(list_sort, req)
	}
	return list_sort, nil
}

// parseSortOperator parses sort operator and returns respective operator defined by fileretreiver.proto
func parseSortOperator(op string) (*fileretreiver.ListFileMetadataRequest_ListFileSort_SortOrder, error) {
	var operator fileretreiver.ListFileMetadataRequest_ListFileSort_SortOrder

	switch op {
	case "ASC":
		operator = fileretreiver.ListFileMetadataRequest_ListFileSort_ASC
	case "DESC":
		operator = fileretreiver.ListFileMetadataRequest_ListFileSort_DESC
	default:
		return nil, fmt.Errorf("Invalid Sort Operation Used")
	}

	return &operator, nil
}

// parseDateToEpoch parses date argument
// It returns unix epoch of date entered and error if occured
// Date format should be in RFC3339 ie yyyy-mm-dd
// for any valid date in RFC3339 format time selected will be midnight in UTC
// Eg: Input date: 2021-04-13
//     Complete date-time in RFC3339 :  2021-04-13 00:00:00 +0000 UTC
//     Unix Epoch: 1618272000
func parseDateToEpoch(date string) (string, error) {
	var (
		year  int
		month int
		day   int
	)
	dateMatch_1, _ := regexp.Compile("^[0-9]{4}-(1[0-2]|0[1-9])-(3[01]|[12][0-9]|0[1-9])$")
	INV_DATE := "Invalid Date format. \n Valid formats: \n yyyy-mm-dd \n"

	if dateMatch_1.MatchString(date) {

		const shortForm = "2006-01-02"
		dt, _ := time.Parse(shortForm, date)

		year = dt.Year()
		month = int(dt.Month())
		day = dt.Day()

	} else {
		return "", fmt.Errorf(INV_DATE)
	}

	time_ := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return strconv.FormatInt(time_.Unix(), 10), nil
}
