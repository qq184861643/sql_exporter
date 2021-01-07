package sql_exporter

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/free/sql_exporter/config"
	"github.com/free/sql_exporter/errors"
	//log "github.com/golang/glog"
	"regexp"
	"strconv"
	"strings"
)

//var ruleMetrics map[string]map[string]string = make(map[string]map[string]string)
//var tmpMap map[string]map[string]string

// Query wraps a sql.Stmt and all the metrics populated from it. It helps extract keys and values from result rows.
type Query struct {
	config         *config.QueryConfig
	metricFamilies []*MetricFamily
	// columnTypes maps column names to the column type expected by metrics: key (string) or value (float64).
	columnTypes columnTypeMap
	logContext  string

	conn *sql.DB
	stmt *sql.Stmt
}

type columnType int
type columnTypeMap map[string]columnType

const (
	columnTypeKey   = 1
	columnTypeValue = 2
)

// NewQuery returns a new Query that will populate the given metric families.
func NewQuery(logContext string, qc *config.QueryConfig, metricFamilies ...*MetricFamily) (*Query, errors.WithContext) {
	logContext = fmt.Sprintf("%s, query=%q", logContext, qc.Name)

	columnTypes := make(columnTypeMap)

	for _, mf := range metricFamilies {
		for _, kcol := range mf.config.KeyLabels {
			if err := setColumnType(logContext, kcol, columnTypeKey, columnTypes); err != nil {
				return nil, err
			}
		}
		for _, vcol := range mf.config.Values {
			if err := setColumnType(logContext, vcol, columnTypeValue, columnTypes); err != nil {
				return nil, err
			}
		}
	}

	q := Query{
		config:         qc,
		metricFamilies: metricFamilies,
		columnTypes:    columnTypes,
		logContext:     logContext,
	}
	return &q, nil
}

// setColumnType stores the provided type for a given column, checking for conflicts in the process.
func setColumnType(logContext, columnName string, ctype columnType, columnTypes columnTypeMap) errors.WithContext {
	previousType, found := columnTypes[columnName]
	if found {
		if previousType != ctype {
			return errors.Errorf(logContext, "column %q used both as key and value", columnName)
		}
	} else {
		columnTypes[columnName] = ctype
	}
	return nil
}

// Collect is the equivalent of prometheus.Collector.Collect() but takes a context to run in and a database to run on.
func (q *Query) Collect(ctx context.Context, conn *sql.DB, ch chan<- Metric) {
	if ctx.Err() != nil {
		ch <- NewInvalidMetric(errors.Wrap(q.logContext, ctx.Err()))
		return
	}
	rows, err := q.run(ctx, conn)
	if err != nil {
		// TODO: increment an error counter
		ch <- NewInvalidMetric(err)
		return
	}
	defer rows.Close()

	dest, err := q.scanDest(rows)
	if err != nil {
		// TODO: increment an error counter
		ch <- NewInvalidMetric(err)
		return
	}
	for rows.Next() {
		row, err := q.scanRow(rows, dest)
		if err != nil {
			ch <- NewInvalidMetric(err)
			continue
		}
		for _, mf := range q.metricFamilies {
			if len(mf.config.Rules) != 0 {
				execRules(*mf, row)
			}
			mf.Collect(row, ch)
		}
	}
	if err1 := rows.Err(); err1 != nil {
		ch <- NewInvalidMetric(errors.Wrap(q.logContext, err1))
	}
}

// run executes the query on the provided database, in the provided context.
func (q *Query) run(ctx context.Context, conn *sql.DB) (*sql.Rows, errors.WithContext) {
	if q.conn != nil && q.conn != conn {
		panic(fmt.Sprintf("[%s] Expecting to always run on the same database handle", q.logContext))
	}

	if q.stmt == nil {
		stmt, err := conn.PrepareContext(ctx, q.config.Query)
		if err != nil {
			return nil, errors.Wrapf(q.logContext, err, "prepare query failed")
		}
		q.conn = conn
		q.stmt = stmt
	}
	rows, err := q.stmt.QueryContext(ctx)
	return rows, errors.Wrap(q.logContext, err)
}

// scanDest creates a slice to scan the provided rows into, with strings for keys, float64s for values and interface{}
// for any extra columns.
func (q *Query) scanDest(rows *sql.Rows) ([]interface{}, errors.WithContext) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(q.logContext, err)
	}

	// Create the slice to scan the row into, with strings for keys and float64s for values.
	dest := make([]interface{}, 0, len(columns))
	have := make(map[string]bool, len(q.columnTypes))
	//for i, column := range columns {
	for _, column := range columns {
		switch q.columnTypes[column] {
		case columnTypeKey:
			dest = append(dest, new(string))
			have[column] = true
		case columnTypeValue:
			dest = append(dest, new(sql.RawBytes))
			have[column] = true
		default:
			/*if column == "" {
				log.Warningf("[%s] Unnamed column %d returned by query", q.logContext, i)
			} else {
				log.Warningf("[%s] Extra column %q returned by query", q.logContext, column)
			}*/
			dest = append(dest, new(sql.RawBytes))
		}
	}

	// Not all requested columns could be mapped, fail.
	if len(have) != len(q.columnTypes) {
		missing := make([]string, 0, len(q.columnTypes)-len(have))
		for c := range q.columnTypes {
			if !have[c] {
				missing = append(missing, c)
			}
		}
		return nil, errors.Errorf(q.logContext, "column(s) %q missing from query result", missing)
	}

	return dest, nil
}

// scanRow scans the current row into a map of column name to value, with string values for key columns and float64
// values for value columns, using dest as a buffer.
func (q *Query) scanRow(rows *sql.Rows, dest []interface{}) (map[string]interface{}, errors.WithContext) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(q.logContext, err)
	}

	// Scan the row content into dest.
	if err := rows.Scan(dest...); err != nil {
		return nil, errors.Wrapf(q.logContext, err, "scanning of query result failed")
	}

	// Pick all values we're interested in into a map.
	result := make(map[string]interface{}, len(q.columnTypes))
	for i, column := range columns {
		if q.columnTypes[column] == columnTypeKey {
			result[column] = *dest[i].(*string)
		} else {
			if value, ok := parseStatus(*dest[i].(*sql.RawBytes)); ok { // Silently skip unparsable values.
				result[column] = value
			} else {
				result[column] = string(*dest[i].(*sql.RawBytes))
			}
		}
	}
	return result, nil
}

func parseStatus(data sql.RawBytes) (float64, bool) {
	var logRE = regexp.MustCompile(`.+\.(\d+)$`)
	if bytes.Equal(data, []byte("Yes")) || bytes.Equal(data, []byte("ON")) {
		return 1, true
	}
	if bytes.Equal(data, []byte("No")) || bytes.Equal(data, []byte("OFF")) {
		return 0, true
	}
	// SHOW SLAVE STATUS Slave_IO_Running can return "Connecting" which is a non-running state.
	if bytes.Equal(data, []byte("Connecting")) {
		return 0, true
	}
	// SHOW GLOBAL STATUS like 'wsrep_cluster_status' can return "Primary" or "non-Primary"/"Disconnected"
	if bytes.Equal(data, []byte("Primary")) {
		return 1, true
	}
	if strings.EqualFold(string(data), "non-Primary") || bytes.Equal(data, []byte("Disconnected")) {
		return 0, true
	}
	if logNum := logRE.Find(data); logNum != nil {
		value, err := strconv.ParseFloat(string(logNum), 64)
		return value, err == nil
	}
	value, err := strconv.ParseFloat(string(data), 64)
	return value, err == nil
}

func execRules(mf MetricFamily, row map[string]interface{}) int {
	for _, rule := range mf.config.Rules {
		key := ""
		for _, label := range rule.SourceLabels {
			//fmt.Println(mf.Name(), label, row[label])
			//key的值为source_labels中label对应的值的拼接字符串，以;分隔
			key = key + ";"
			if k, ok := row[label].(string); ok {
				key = key + k
			} else {
				key = key + " interface assertation error at operate key"
			}
		}
		switch strings.ToLower(rule.Action) {
		case "relabel":
			//根据key查询全局map，将查询结果赋予目标标签列
			{
				row[rule.TargetLabel] = ruleMetrics[rule.RuleMetric][key]
			}
		default: //case "writelabel":
			//将规则指标的目标标签列的值写入临时全局map变量中
			//该collector的所有查询结束后，会将临时全局map的值赋值给全局map，并置空临时map
			{
				if tmpMap[mf.Name()] == nil {
					tmpMap[mf.Name()] = make(map[string]string)
				}
				if v, ok := row[rule.TargetLabel].(string); ok {
					tmpMap[mf.Name()][key] = v
				} else {
					tmpMap[mf.Name()][key] = mf.Name() + " interface assertation error at writelabel rule"
				}
			}
		}
	}
	return 0
}
