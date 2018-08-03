package com.huoli.statictag.model

import java.io.{DataInput, DataOutput}
import java.sql.{PreparedStatement, ResultSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.orc.{ OrcNewInputFormat, OrcStruct}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.db.{DBOutputFormat, DBWritable}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
object ExportHiveToMysql {

  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    import org.apache.hadoop.util.GenericOptionsParser
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    import org.apache.hadoop.mapreduce.Job


    conf.set("orc.mapred.output.schema", "struct<name:string,mobile:string>")



    val job = Job.getInstance(conf, "ORC Test")


    job.setJarByClass(ExportHiveToMysql.getClass)
    job.setMapperClass(classOf[OrcFileMapper])
    job.setNumReduceTasks(0)
    job.setInputFormatClass(classOf[OrcNewInputFormat])
    job.setOutputFormatClass(classOf[DBOutputFormat[ResidentCityWritable,ResidentCityWritable]])

    job.setMapOutputKeyClass(classOf[ResidentCityWritable])
    job.setMapOutputKeyClass(classOf[ResidentCityWritable])

    FileInputFormat.setInputPaths(job, new Path("/hive/warehouse/tmp.db/tmp_weight_city_phone"))

    //OrcInputFormat.addInputPath(job, new Path(""))
    //  lib.db.DBOutputFormat
    DBOutputFormat.setOutput(job,"resident_city_new","user_id","city","rk")

    System.exit(
      if(job.waitForCompletion(true))
           0
      else
          -1
    )

  }

  class OrcFileMapper extends Mapper[NullWritable, OrcStruct, ResidentCityWritable,ResidentCityWritable]{

    override def map(key: NullWritable, value: OrcStruct, context: Mapper[NullWritable, OrcStruct, ResidentCityWritable, ResidentCityWritable]#Context): Unit = {
       /* val user_id=value.getFieldValue(1).asInstanceOf[String]
        val city=value.getFieldValue(2).asInstanceOf[String]
        val rk=value.getFieldValue(3).asInstanceOf[Int]
        context.write(new ResidentCityWritable(user_id,city,rk),null)*/
    }
  }
  class ResidentCityWritable(var user_id:String,var city:String,var rk:Int) extends  Writable with DBWritable{
    override def write(dataOutput: DataOutput): Unit = {
            dataOutput.writeUTF(this.user_id)
            dataOutput.writeUTF(this.city)
            dataOutput.writeInt(this.rk)
    }

    override def readFields(dataInput: DataInput): Unit = {
          this.user_id=dataInput.readUTF()
          this.city=dataInput.readUTF()
          this.rk=dataInput.readInt()
    }

    override def write(preparedStatement: PreparedStatement): Unit = {
         preparedStatement.setString(1,this.user_id)
         preparedStatement.setString(2,this.city)
         preparedStatement.setInt(3,this.rk)
    }

    override def readFields(resultSet: ResultSet): Unit = {
           this.user_id=resultSet.getString(1)
           this.city=resultSet.getString(2)
           this.rk=resultSet.getInt(3)
    }


    override def toString = s"ResidentCityWritable($user_id, $city, $rk)"
  }

}
