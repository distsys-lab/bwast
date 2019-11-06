package bftsmart.demo.map;

import java.io.Console;
import java.util.Set;

public class MapNoninteractiveClient {
	
	public static void main(String[] args) {
		if(args.length < 4) {
			System.out.println("Usage: demo.map.MapNoInteractiveClient <client id> <key size> <value size> <times>");
		}
		
		int clientId = Integer.parseInt(args[0]);
		int keySize = Integer.parseInt(args[1]);
		int valueSize = Integer.parseInt(args[2]);
		int times = Integer.parseInt(args[3]);
		MapClient<String, String> map = new MapClient<>(clientId);
		for(int i = 0; i < times; i++) {
			String value = generateRandomString(valueSize);
			for(int j = 0; j < keySize; j++) {
				String key = Integer.toString(i);
				map.put(key, value);
			}
			System.out.println("put " + (i + 1) + " / " + times);
		}
		System.out.println("All tasks are done.");
	}

	private static String generateRandomString(int size) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < size;i++) {
			sb.append((char)(Math.random() * 26) + 'A');
		}
		return sb.toString();
	}

}
