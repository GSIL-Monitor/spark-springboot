package com.swjuyhz.sample.kryo;

import com.esotericsoftware.kryo.Kryo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.serializer.KryoRegistrator;

public class MyRegistrator implements KryoRegistrator {

	@Override
	public void registerClasses(Kryo arg0) {
		arg0.register(ConsumerRecord.class);
	}

}
