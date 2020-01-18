package com.flink.demo;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapred.JobConf;

/**
 * K-Means clustering in Flink with data from MongoDB
 */
public class MongdbJob {


    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//mongodb+srv://gatewayuser:VCUOTuAuBEPeEKDf@cluster0-eagup.azure.mongodb.net/test?retryWrites=true&w=majority&authSource=admin

        JobConf jobConf = new JobConf();
        jobConf.set("mongo.input.uri", "");


		jobConf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()//	org.apache.hadoop.fs.LocalFileSystem.class.getName()
		);
		jobConf.set("mongo.output.uri", "mongodb://172.24.0.2/zips.statecities");

		HadoopInputFormat<BSONWritable, BSONWritable> hdIf =
				new HadoopInputFormat<BSONWritable, BSONWritable>(new MongoInputFormat(),
						BSONWritable.class, BSONWritable.class, jobConf);

        DataSet<Tuple2<BSONWritable, BSONWritable>> input = env.createInput(hdIf);


        MongoOutputFormat<BSONWritable, BSONWritable> mongoOutputFormat = new MongoOutputFormat<BSONWritable, BSONWritable>();

        DataSet<Tuple2<BSONWritable, BSONWritable>> finalData = input.map(new MapFunction<Tuple2<BSONWritable, BSONWritable>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple2<BSONWritable, BSONWritable> bsonWritableBSONWritableTuple2) throws Exception {
                return new Tuple2<String, String>(bsonWritableBSONWritableTuple2.f1.getDoc().get("_id").toString(),
                        bsonWritableBSONWritableTuple2.f1.getDoc().get("zip").toString());
            }
        }).map(new MapFunction<Tuple2<String, String>, Tuple2<BSONWritable, BSONWritable>>() {
            @Override
            public Tuple2<BSONWritable, BSONWritable> map(Tuple2<String, String> stringStringTuple2) throws Exception {
                DBObject builder = BasicDBObjectBuilder.start()
                        .add("_id", stringStringTuple2.f1)
                        .get();

                DBObject builderBedType = BasicDBObjectBuilder.start()
                        .add("zip", stringStringTuple2.f1)
                        .get();


                return new Tuple2<BSONWritable, BSONWritable>(new BSONWritable(builder), new BSONWritable(builder));
            }
        });

        // emit result (this works only locally)
        finalData.output(new HadoopOutputFormat<BSONWritable, BSONWritable>(mongoOutputFormat, jobConf));

        // execute program
        env.execute("KMeans Example");
    }


}
