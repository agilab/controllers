package controllers

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"encoding/json"

	"github.com/gorilla/mux"
	"github.com/prest/config"
)
type countStruct struct{
	Sum int
}
const OFFSETTAG string  = "offset"
const LIMITTAG string  = "limit"
const DefultOffset int  = 0
const DefultLimit int  =  1000
// ExecuteScriptQuery is a function to execute and return result of script query
func ExecuteScriptQuery(rq *http.Request, queriesPath string, script string)([]byte,error) {
	//log.Printf("script:%+v\n",script)
	sqlPath, err := config.PrestConf.Adapter.GetScript(rq.Method, queriesPath, script)
	if err != nil {
		err = fmt.Errorf("could not get script %s/%s, %+v", queriesPath, script, err)
		return nil, err
	}
	//fmt.Printf("rq.URL.Query():%+v \n", rq.URL.Query())
	sql, values, err := config.PrestConf.Adapter.ParseScript(sqlPath, rq.URL.Query())
	if err != nil {
		err = fmt.Errorf("could not parse script %s/%s, %+v", queriesPath, script, err)
		return nil, err
	}
	log.Printf("sql:%+v\n",sql)
	//test
	// A,B,C:=config.PrestConf.Adapter.WhereByRequest(rq,10)
	// fmt.Printf("%+v  %+v  %+v\n",A,B,C )
	// fmt.Printf("A:%+v \n", A)
	// fmt.Printf("values:%+v \n",values )
	//test
	_ , _ ,PaginateStr := GetPaginateStr(rq)
	paginateSql := sql + PaginateStr
	sc := config.PrestConf.Adapter.ExecuteScripts(rq.Method, paginateSql, values)
	if sc.Err() != nil {
		err = fmt.Errorf("could not execute sql %+v, %s", sc.Err(), sql)
		return nil , err
	}
	return sc.Bytes(), nil
}

func ExecuteCountQuery(rq *http.Request, queriesPath string, script string) (int, error) {
	sqlPath, err := config.PrestConf.Adapter.GetScript(rq.Method, queriesPath, script)
	if err != nil {
		err = fmt.Errorf("could not get script %s/%s, %+v", queriesPath, script, err)
		return 0, err
	}
	sql, values, err := config.PrestConf.Adapter.ParseScript(sqlPath, rq.URL.Query())
	if err != nil {
		err = fmt.Errorf("could not parse script %s/%s, %+v", queriesPath, script, err)
		return 0, err
	}
	countSql,countErr := GetCountStr(sql)
	if countErr!=nil{
		return 0, countErr
	}
	countSc := config.PrestConf.Adapter.ExecuteScripts(rq.Method, countSql, values)
	if countSc.Err() != nil {
		err = fmt.Errorf("could not execute sql %+v, %s", countSc.Err(), countSql)
		return 0, err
	}
	fmt.Println(countSc)
	return getCountNumFromStruct(countSc.Bytes())
}
func getCountNumFromStruct(countSc []byte)(int ,error){
	var countStructArr []countStruct
	unmarshalErr:=json.Unmarshal(countSc,&countStructArr)
	if unmarshalErr != nil {
		return 0 , unmarshalErr
	}
	if len(countStructArr)==0{
		return 0 , nil
	}
	return countStructArr[0].Sum ,nil
}
// ExecuteFromScripts is a controller to peform SQL in scripts created by users
func ExecuteFromScripts(w http.ResponseWriter, r *http.Request) {
	log.Println("AC")
	vars := mux.Vars(r)
	queriesPath := vars["queriesLocation"]
	script := vars["script"]
	result, err := ExecuteScriptQuery(r, queriesPath, script)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.Header.Get("Prefer")=="count=exact"{
		myOffset , myLimit , _ := GetPaginateStr(r)
		myCount, err := ExecuteCountQuery(r, queriesPath, script)
		if err != nil {
			w.Header().Set("content-range",getContentRangeStr(myOffset,myLimit,-1))
			log.Println("err:",err)
		}else{
			// log.Println("myOffset:",myOffset)
			// log.Println("myLimit:",myLimit)
			// log.Println("myCount:",myCount)
			w.Header().Set("content-range",getContentRangeStr(myOffset,myLimit,myCount))
		}
	}
	w.Write(result)
}

func getContentRangeStr(myOffset,myLimit,myCount int)string{
	ContentRangeFmtStr := "%d-%d/%d" //0-4/6
	EmptyContentRangeFmtStr := "*/%d" //*/6
	myStart := myOffset
	var myEnd int
	if myLimit < myCount{
		myEnd = myOffset + myLimit -1
	}else{
		myEnd = myOffset + myCount -1
	}
	if myCount > 0{
		return fmt.Sprintf(ContentRangeFmtStr,myStart,myEnd,myCount)
	}else{
		return fmt.Sprintf(EmptyContentRangeFmtStr,myCount)
	}
}

func GetCountStr(myOrderSql string)(string,error){
	var myCountSql string
	myCountSql = `select 
				  count(1)
				  from (` + myOrderSql + `) son`
	return myCountSql,nil
}

func GetPaginateStr(r *http.Request)(offset int,limit int, paginatedQuery string){
	values := r.URL.Query()
	paginatedQuery=" "
	offset = 0
	limit = 1000
	var err error 
	if _, ok := values[OFFSETTAG]; ok {
		offset, err = strconv.Atoi(values[OFFSETTAG][0])
		if err != nil {
			offset = 0
		}else{
			paginatedQuery = paginatedQuery + fmt.Sprintf("OFFSET %s",values[OFFSETTAG][0])
		}
	}else{
		
	}
	if _, ok := values[LIMITTAG]; ok {
		limit, err = strconv.Atoi(values[LIMITTAG][0])
		if err != nil {
			limit = 1000
		}else{
			paginatedQuery = paginatedQuery + fmt.Sprintf("LIMIT %s",values[LIMITTAG][0])
		}
	}else{
		
	}
	return 
}
