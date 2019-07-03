package com.anshu

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

class fqxudf extends Serializable {
	def transaction = (s: String) => s match {
		case"MD" => "FSS    " 
		case "RGT" | "MPT" => "VPE   "
		case "FQQ"|"FXT"|"FXU"|"FXV"|"FXZ" => "FSC   "
		case "FQP" => "FSI   "
		case "FQD"|"FQF" => "FSD   "
		case x if s.take(2) == "FQ" => "FST   "
		case "FXA"|"FXB"|"FXL"|"FXR" => "FSB   "
		case "ANP" => "ABNP  "
		case "MTC" => "MTPC  "
		case "MPC"|"MTP"|"SOC"|"SOT"|"CTB"|"ELI"|"SPR"|"CAL"|"FFP"|"CCP"|"FCP"|"AWC"|"AWT"|"CCL"|"YFD" => s
		case "FXP"|"FXX" => "FSP   "
		case "FXC" => "FCR   "
		case "FXE"|"FXO"|"FXF"|"FXI"|"FXQ"|"TTP" => "FATP  "
		case "FXM" => "FSM   "
		case "MGP" => "MCP   "
		case "XCL" => "XCAL  "
		case "XSP" => "MSP   "
		case x if s.take(2) == "FR" => "FST   "
		case x if s.take(2) == "WR" => "PRT   "
		case " "|""|"  " => "UNKNOW"
		case _ => s
	}
	def decode = (c: String) => { if (c.length == 12) c else "" }
	def vpr = (s: String) => s match {
		case "ABNP"|"AWC"|"AWT"|"CAL"|"CCL"|"CCP"|"CTB"|"ELI"|"FCP"|"FCR"|"FFP"|"MCP"|"MPC"|"MSP"|"MTP"|"MTPC"|"SOC"|"SOT"|"SPR"|"VPE"|"XCAL"|"YFD" => "Y"
		case _ => "N"
	}
}
obejct MyUdf extends App {
  val infile = "/mapr/xxxx/yyyy/zzzzz/test.csv"
  val events = spark.read.format("csv").
                option("header", false).
                option("mode","FAILFAST").
                option("delimiter","^").
                option("inferSchema",true).
                option("timestampFormat", "yyyy/MM/dd HH:mm:ss").
                load(infile)
  val sqlContext = new SQLContext(sc)
  val d = new fqxudf
  sqlContext.udf.register("trans",d.transaction)
  sqlContext.udf.register("dcod",d.decode)
  sqlContext.udf.register("vpr",d.vpr)
  events.registerTempTable("event")
  def transaction: DataFrame = sqlContext.sql("select *, trans(_c5) as trx ,
                                               substr(dcod(_c83),9,3) as user_cat, 
                                               substr(dcod(_c83),4,3) as access_type, 
                                               substr(dcod(_c83),7,3) as product,
                                               vpr(trans(_c5)) as vprec from event")
}
