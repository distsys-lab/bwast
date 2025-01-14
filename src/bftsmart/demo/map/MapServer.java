package bftsmart.demo.map;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.ModifiedDefaultSingleRecoverable;

public class MapServer<K, V> extends ModifiedDefaultSingleRecoverable {

	private int KEY_NUM = 500;
	private int VALUE_SIZE = 1048576;

	private Map<K, V> replicaMap;
	private Logger logger;

	public MapServer(int id) {
		replicaMap = /*createInitialMap();*/new TreeMap<>();
		logger = Logger.getLogger(MapServer.class.getName());
		new ServiceReplica(id, this, this);
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Usage: demo.map.MapServer <server id>");
			System.exit(-1);
		}
		new MapServer<String, String>(Integer.parseInt(args[0]));
	}

	@SuppressWarnings("unchecked")
	@Override
	public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
		byte[] reply = null;
		K key = null;
		V value = null;
		boolean hasReply = false;
		try (ByteArrayInputStream byteIn = new ByteArrayInputStream(command);
				ObjectInput objIn = new ObjectInputStream(byteIn);
				ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {
			MapRequestType reqType = (MapRequestType)objIn.readObject();
			switch (reqType) {
				case PUT:
					key = (K)objIn.readObject();
					value = (V)objIn.readObject();

					V oldValue = replicaMap.put(key, value);
					if (oldValue != null) {
						objOut.writeObject(oldValue);
						hasReply = true;
					}
					break;
				case GET:
					key = (K)objIn.readObject();
					value = replicaMap.get(key);
					if (value != null) {
						objOut.writeObject(value);
						hasReply = true;
					}
					break;
				case REMOVE:
					key = (K)objIn.readObject();
					value = replicaMap.remove(key);
					if (value != null) {
						objOut.writeObject(value);
						hasReply = true;
					}
					break;
				case SIZE:
					int size = replicaMap.size();
					objOut.writeInt(size);
					hasReply = true;
					break;
				case KEYSET:
					keySet(objOut);
					hasReply = true;
					break;
			}
			if (hasReply) {
				objOut.flush();
				byteOut.flush();
				reply = byteOut.toByteArray();
			} else {
				reply = new byte[0];
			}

		} catch (IOException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Ocurred during map operation execution", e);
		}
		return reply;
	}

	@SuppressWarnings("unchecked")
	@Override
	public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
		byte[] reply = null;
		K key = null;
		V value = null;
		boolean hasReply = false;

		try (ByteArrayInputStream byteIn = new ByteArrayInputStream(command);
				ObjectInput objIn = new ObjectInputStream(byteIn);
				ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut);) {
			MapRequestType reqType = (MapRequestType)objIn.readObject();
			switch (reqType) {
				case GET:
					key = (K)objIn.readObject();
					value = replicaMap.get(key);
					if (value != null) {
						objOut.writeObject(value);
						hasReply = true;
					}
					break;
				case SIZE:
					int size = replicaMap.size();
					objOut.writeInt(size);
					hasReply = true;
					break;
				case KEYSET:
					keySet(objOut);
					hasReply = true;
					break;
				default:
					logger.log(Level.WARNING, "in appExecuteUnordered only read operations are supported");
			}
			if (hasReply) {
				objOut.flush();
				byteOut.flush();
				reply = byteOut.toByteArray();
			} else {
				reply = new byte[0];
			}
		} catch (IOException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Ocurred during map operation execution", e);
		}

		return reply;
	}

	private void keySet(ObjectOutput out) throws IOException, ClassNotFoundException {
		Set<K> keySet = replicaMap.keySet();
		int size = replicaMap.size();
		out.writeInt(size);
		for (K key : keySet)
			out.writeObject(key);
	}

	@Override
	public byte[] getSnapshot() {
		try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
				ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
			objOut.writeObject(replicaMap);
			return byteOut.toByteArray();
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error while taking snapshot", e);
		}
		return new byte[0];
	}

	@SuppressWarnings("unchecked")
	@Override
	public void installSnapshot(byte[] state) {
		try (ByteArrayInputStream byteIn = new ByteArrayInputStream(state);
				ObjectInput objIn = new ObjectInputStream(byteIn)) {
			replicaMap = (Map<K, V>)objIn.readObject();
		} catch (IOException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Error while installing snapshot", e);
		}
	}

	private TreeMap<K,V> createInitialMap() {
		TreeMap<K,V> treeMap = new TreeMap<>();
		for(int i = 0; i < KEY_NUM; i++) {
			K key = (K)Integer.toString(i);
			V value = (V)generateString(VALUE_SIZE);
			treeMap.put(key, value);
		}
		return treeMap;
	}

	private static String generateString(int size) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < size; i++) {
			sb.append('A');
		}
		return sb.toString();
	}
}