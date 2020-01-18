package com.flink.demo;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoInputFormat;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;

/**
 * K-Means clustering in Flink with data from MongoDB
 */
public class MongdbJob {

	public static DataSet<Tuple2<BSONWritable, BSONWritable>> readFromMongo(ExecutionEnvironment env, String uri) {
		JobConf conf = new JobConf();
		conf.set("mongo.input.uri", uri);
		MongoInputFormat mongoInputFormat = new MongoInputFormat();


		return env.createInput(HadoopInputs.createHadoopInput( mongoInputFormat, BSONWritable.class, BSONWritable.class, conf));
	}

	public static void writeToMongo(DataSet<Tuple2<BSONWritable, BSONWritable>> result, String uri) {
		JobConf conf = new JobConf();
		conf.set("mongo.output.uri", uri);
		MongoOutputFormat<BSONWritable, BSONWritable> mongoOutputFormat = new MongoOutputFormat<BSONWritable, BSONWritable>();
		result.output(new HadoopOutputFormat<BSONWritable, BSONWritable>(mongoOutputFormat, conf));
	}

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		HadoopInputFormat<BSONWritable, BSONWritable> hdIf =
				new HadoopInputFormat<BSONWritable, BSONWritable>(new MongoInputFormat(),
						BSONWritable.class, BSONWritable.class, new JobConf());
		hdIf.getJobConf().set("mongo.input.uri", "mongodb://127.0.0.1/zips.zips");
		DataSet<Tuple2<BSONWritable, BSONWritable>> input = env.createInput(hdIf);


		JobConf conf = new JobConf();
		conf.set("mongo.output.uri", "mongodb://127.0.0.1/zips.statecities");

		MongoOutputFormat<BSONWritable, BSONWritable> mongoOutputFormat = new MongoOutputFormat<BSONWritable, BSONWritable>();

		DataSet<Tuple2<BSONWritable, BSONWritable>> finalData = input.map(new MapFunction<Tuple2<BSONWritable, BSONWritable>, Tuple2<String, String>>() {
			@Override
			public Tuple2<String, String> map(Tuple2<BSONWritable, BSONWritable> bsonWritableBSONWritableTuple2) throws Exception {
				return new Tuple2<String, String>(bsonWritableBSONWritableTuple2.f1.toString(),
						bsonWritableBSONWritableTuple2.f1.toString());
			}
		}).map(new MapFunction<Tuple2<String, String>, Tuple2<BSONWritable, BSONWritable>>() {
			@Override
			public Tuple2<BSONWritable, BSONWritable> map(Tuple2<String, String> stringStringTuple2) throws Exception {
				DBObject builder = BasicDBObjectBuilder.start()
						.add("city", stringStringTuple2.f1)
						.get();


				return new Tuple2<BSONWritable, BSONWritable>( new BSONWritable(builder), new BSONWritable(builder));
			}
		});

		finalData.print();


		MapredMongoConfigUtil.setOutputURI( conf, "mongodb://127.0.0.1/zips.statecities");
		MongoConfigUtil.setOutputURI( conf, "mongodb://127.0.0.1/zips.statecities");

		// emit result (this works only locally)
		finalData.output(new HadoopOutputFormat<BSONWritable, BSONWritable>(mongoOutputFormat, conf));

		// execute program
		env.execute("KMeans Example");
	}



}
