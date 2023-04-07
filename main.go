package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var sourceDir string
var targetDir string
var days string

func main() {
	flag.StringVar(&sourceDir, "source", "./testdata", "sqlite文件所在的目录, 例如:/data/sqlite/1045")
	flag.StringVar(&targetDir, "target", "./testdata", "")
	flag.StringVar(&days, "days", "-24h", "")

	flag.Parse()

	logrus.SetLevel(logrus.TraceLevel)

	logrus.Debugf("source:%s,target:%s", sourceDir, targetDir)

	// /data/sqlite/{platid，广东移动=1045}/{company_id}/EsdAttrReport/{yyyy-MM}/{duuid}.db
	// /data/sqlite/{platid，广东移动=1045}/{company_id}/EmcuReport/{yyyy-MM}/{eleid}.db
	join := filepath.Join(sourceDir, "/*/*/*/*.db")
	logrus.Debugf("scan db in path:%s", join)
	dbs, err := filepath.Glob(join)
	if err != nil {
		panic(err)
	}
	total := len(dbs)
	logrus.Debugf("scan finish,db count:%v ,convert begin...", total)
	for i, dbFile := range dbs {
		if err := convert(i, total, dbFile); err != nil {
			logrus.Errorf("[%d/%d][sqlite]convert file %s to parquet error:%v", i+1, total, dbFile, err)
			continue
		}

	}

}

func convert(i int, total int, dbFile string) error {
	var err error
	logrus.Debugf("[%d/%d]begin convert %s", i+1, total, dbFile)
	if err != nil {
		logrus.Errorf("[%d/%d][sqlite]open %s error:%v ,skip this file", i+1, total, dbFile, err.Error())
		return err
	}
	executor, err := getDataQueryExecutor(dbFile)
	if err != nil {
		logrus.Errorf("[%d/%d][sqlite]unsupport this path %s(not contain EsdAttrReport or EmcuReport),skip this file", i+1, total, dbFile)
		return err
	}
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		logrus.Errorf("[%d/%d][sqlite]find executor error:%v", i+1, total, err)
		return err
	}
	defer db.Close()
	err = executor(db)
	if err != nil {
		logrus.Errorf("[%d/%d][sqlite]call executor error:%v", i+1, total, err)
		return err
	}
	return nil
}

type Esd_port_attr_report_his struct {
	Id         int    `parquet:"name=id, type=INT64, encoding=PLAIN"`
	Duuid      string `parquet:"name=duuid, type=BYTE_ARRAY, convertedtype=UTF8"`
	Esdseq     string `parquet:"name=esdseq, type=BYTE_ARRAY, convertedtype=UTF8"`
	Esdtype    string `parquet:"name=esdtype, type=BYTE_ARRAY, convertedtype=UTF8"`
	Esdvalue   string `parquet:"name=esdvalue, type=BYTE_ARRAY, convertedtype=UTF8"`
	Colltime   string `parquet:"name=colltime, type=BYTE_ARRAY, convertedtype=UTF8"`
	Createtime string `parquet:"name=createtime, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type T_emcu_report_his struct {
	Id         int    `parquet:"name=id, type=INT64, encoding=PLAIN"`
	Eleid      string `parquet:"name=eleid, type=BYTE_ARRAY, convertedtype=UTF8"`
	Datatype   int    `parquet:"name=datatype, type=INT64, encoding=PLAIN"`
	Datevalue  string `parquet:"name=datevalue, type=BYTE_ARRAY, convertedtype=UTF8"`
	Reporttime string `parquet:"name=reporttime, type=BYTE_ARRAY, convertedtype=UTF8"`
	Status     string `parquet:"name=status, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type Executor func(db *sql.DB) error

func getDataQueryExecutor(file string) (Executor, error) {
	//前一天
	duration, err := time.ParseDuration(days)
	if err != nil {
		logrus.Errorf("parse duration error: %v", err)
		return nil, err
	}
	d := time.Now().Add(duration)
	format := d.Format("2006-01-02 15:04:05")
	outfilePrefix := d.Format("2006-01-02")

	outfilename := strings.ReplaceAll(filepath.Base(file), ".db", ".parquet.snappy")
	outDir := filepath.Join(targetDir, outfilePrefix)
	err = os.MkdirAll(outDir, 0755)
	if err != nil {
		return nil, err
	}
	outfile := filepath.Join(targetDir, outfilePrefix, outfilename)

	//三合一
	if strings.Contains(file, "EsdAttrReport") {
		f := func(db *sql.DB) error {
			rows, err := db.Query(`select * from ESD_PORT_ATTR_REPORT_HIS where colltime >= ? `, format)
			if err != nil {
				logrus.Errorf("[sqlite] execute query error:%v", err)
				return err
			}
			defer rows.Close()
			result := make([]*Esd_port_attr_report_his, 0)
			for rows.Next() {
				//获取一行数据
				var id int
				var duuid, esdseq, esdtype, esdvalue, colltime, createtime string

				if err := rows.Scan(&id, &duuid, &esdseq, &esdtype, &esdvalue, &colltime, &createtime); err != nil {
					logrus.Errorf(`[sqlite] scan error:%v`, err)
					return err
				}
				result = append(result, &Esd_port_attr_report_his{
					id, duuid, esdseq, esdtype, esdvalue, colltime, createtime,
				})
			}
			logrus.Debugf("[sqlite]query result data:%d", len(result))
			w, err := os.Create(outfile)
			if err != nil {
				logrus.Errorf("[sqlite] create file error:%v", err)
				return err
			}
			defer w.Close()
			pw, err := writer.NewParquetWriterFromWriter(w, new(Esd_port_attr_report_his), 4)
			if err != nil {
				logrus.Errorf(`[sqlite] create parquet writer error:%v`, err)
				return err
			}
			pw.RowGroupSize = 128 * 1024 * 1024 //128M

			pw.CompressionType = parquet.CompressionCodec_SNAPPY
			for _, datum := range result {
				err := pw.Write(datum)
				if err != nil {
					logrus.Errorf(`[sqlite] write parquet error:%v`, err)
					return err
				}
			}

			if err := pw.WriteStop(); err != nil {
				logrus.Errorf(`[sqlite] write parquet stop error:%v`, err)
				return err
			}
			logrus.Debugf("[ParquetWriter]write data finish")
			return nil
		}
		return f, nil
	}
	//emcu
	if strings.Contains(file, "EmcuReport") {
		f := func(db *sql.DB) error {
			rows, err := db.Query(`select * from t_emcu_report_his where reporttime >= ? and status='1' `, format)
			if err != nil {
				logrus.Errorf(`[sqlite] query emcu report error:%v`, err)
				return err
			}
			defer rows.Close()
			result := make([]*T_emcu_report_his, 0)
			for rows.Next() {
				//获取一行数据
				var id, datatype int
				var eleid, datevalue, reporttime, status string

				if err := rows.Scan(&id, &eleid, &datatype, &datevalue, &reporttime, &status); err != nil {
					logrus.Errorf(`[sqlite] scan emcu report error:%v`, err)
					return err
				}
				result = append(result, &T_emcu_report_his{
					id, eleid, datatype, datevalue, reporttime, status,
				})
			}
			logrus.Debugf("[sqlite]query result data:%d", len(result))
			w, err := os.Create(outfile)
			if err != nil {
				logrus.Errorf(`[sqlite] create emcu report file error:%v`, err)
				return err
			}
			pw, err := writer.NewParquetWriterFromWriter(w, new(T_emcu_report_his), 4)
			if err != nil {
				logrus.Errorf(`[sqlite] create emcu report parquet writer error:%v`, err)
				return err
			}
			pw.RowGroupSize = 128 * 1024 * 1024 //128M
			pw.CompressionType = parquet.CompressionCodec_SNAPPY
			for _, datum := range result {
				err := pw.Write(datum)
				if err != nil {
					logrus.Errorf(`[sqlite] write emcu report parquet error:%v`, err)
					return err
				}
			}
			if err := pw.WriteStop(); err != nil {
				logrus.Errorf(`[sqlite] write emcu report parquet stop error:%v`, err)
				return err
			}
			logrus.Debugf("[ParquetWriter]write data finish")
			return nil

		}
		return f, nil
	}
	return nil, fmt.Errorf("executor not found")
}
