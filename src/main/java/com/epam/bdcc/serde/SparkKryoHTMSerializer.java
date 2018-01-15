package com.epam.bdcc.serde;

import com.epam.bdcc.htm.HTMNetwork;
import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.htm.ResultState;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.serialize.HTMObjectInput;
import org.numenta.nupic.serialize.HTMObjectOutput;
import org.numenta.nupic.serialize.SerialConfig;
import org.numenta.nupic.serialize.SerializerCore;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// SerDe HTM objects using SerializerCore (https://github.com/RuedigerMoeller/fast-serialization)
public class SparkKryoHTMSerializer<T> extends Serializer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkKryoHTMSerializer.class);
    private final SerializerCore htmSerializer = new SerializerCore(SerialConfig.DEFAULT_REGISTERED_TYPES);

    public SparkKryoHTMSerializer() {
        htmSerializer.registerClass(Network.class);
    }

    @Override
    public T copy(Kryo kryo, T original) {
        //TODO : Add implementation for clone, if needed
        return kryo.copy(original);

    }

    @Override
    public void write(Kryo kryo, Output kryoOutput, T t) {
        HTMObjectOutput writer = new HTMObjectOutput(kryoOutput, FSTConfiguration.getDefaultConfiguration());
        try {
            writer.writeObject(t, HTMNetwork.class, MonitoringRecord.class, ResultState.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public T read(Kryo kryo, Input kryoInput, Class<T> aClass) {
        //TODO : Add implementation for deserialization

        try (HTMObjectInput reader = new HTMObjectInput(kryoInput, FSTConfiguration.getDefaultConfiguration())) {
            reader.readObject(aClass);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void registerSerializers(Kryo kryo) {
        kryo.register(HTMNetwork.class);
        kryo.register(MonitoringRecord.class);
        kryo.register(ResultState.class);
        for (Class c : SerialConfig.DEFAULT_REGISTERED_TYPES)
            kryo.register(c, new SparkKryoHTMSerializer<>());
    }
}