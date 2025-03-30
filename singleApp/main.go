package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	lib "github.com/magpierre/operators/shared_library"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

var (
	f = flag.String("file", "", "the file to import")
)

func main() {
	flag.Parse()
	d := lib.CreateDataFrameFromCSV(readCSV(*f))

	columns := []string{
		"median_house_value",
		"total_rooms",
		"ocean_proximity",
		"median_income",
		"households",
		"housing_median_age",
	}

	x, err := d.Project(columns...)
	if err != nil {
		log.Fatal(err)
	}

	x.Transform(" map( housing_median_age, int( replace( # , '.0', '' ) ) )")
	x.Transform(" map( households, int( replace( #,'.0', '' ) ) )")
	x.Transform(" map( median_income, int( replace( replace( #, '.', '' ), '.0', '' ) ) )")
	x.Transform(" map( total_rooms, int( replace( #, '.0', '' ) ) )")
	x.Transform(" map( median_house_value, int( replace( #, '.0', '') ) )")

	fmt.Fprintln(os.Stderr, x.Count())

	y, err := x.Where(" housing_median_age >= 42 ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintln(os.Stderr, y.Count())
	z, err := y.Where(" ocean_proximity == 'NEAR BAY' ")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Fprintln(os.Stderr, z.Count())

	z.Transform("let total_households = reduce( households, #acc + # , 0 ); total_households ")
	z.Transform("let total_rooms2 	  = reduce( total_rooms, #acc + #, 0 ); total_rooms2 ")
	z.RenameColumn("total_rooms2", "summary_total_rooms")
	z.RenameColumn("total_households", "summary_total_households")
	z.Transform("let avg_households   = max(summary_total_households) / len(summary_total_households); avg_households ")

	a, err := z.UnionAll(z)
	if err != nil {
		log.Fatal(err)
	}
	a, err = a.UnionAll(a)
	if err != nil {
		log.Fatal(err)
	}

	a, _ = a.Where("median_income > 4000")

	d1 := lib.CreateDataFrameFromCSV(readCSV("/Users/magnuspierre/Downloads/Adult+Census+Income.csv"))

	fmt.Fprintln(os.Stderr, a.Count())
	lib.PrintDataframe(*a, os.Stdout)
	for k, v := range d1.GetFieldNames() {
		fmt.Println(k, v)
	}

	d1.Transform("map(relationship	, upper(#))")
	d1.Transform("map(occupation	, lower(#))")
	d1.Transform("map(race			, upper(#))")
	d1.Transform("map(sex			, lower(#))")
	d1.Transform("map(education		, lower(#))")
	d1.RenameColumn("native.country", "native_country")
	d1.RenameColumn("capital.loss", "capital_loss")
	d1.RenameColumn("capital.gain", "capital_gain")
	d1.RenameColumn("marital.status", "martial_status")
	d1.RenameColumn("hours.per.week", "hours_per_week")
	d1.RenameColumn("education.num", "education_num")
	d1.Transform("map(native_country, upper(#))")
	d1.Transform("map(hours_per_week, int(#))")
	d1.Transform("map(martial_status, upper(#))")
	d1.Transform("map(capital_loss	, int(#))")
	d1.Transform("map(capital_gain	, int(#))")
	d1.Transform("map(education_num	, int(#))")
	d1.Transform("map(fnlwgt		, int(#))")
	d1.Transform("map(workclass		, lower(#))")
	d1.Transform("map(age			, int(#))")
	lib.PrintDataframe(d1, os.Stdout)

	fr, err := local.NewLocalFileReader("/Users/magnuspierre/Documents/code/DeltaSharingTest/cache/open-delta-sharing.s3.us-west-2.amazonaws.com/samples/nyctaxi_2019/part-00284-6120bfc1-bbad-4d04-9950-63525f7716cc-c000.snappy.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	fmt.Println(pr.GetNumRows())

}
