import map.DistributedMap;
import java.util.Scanner;

public class Main {
    private static String MULTICAST_GROUP = "230.0.0.159";
    private static String CHANNEL_NAME = "MAP_CHANNEL";
    private DistributedMap map;

    public Main(String channelName, String multicastGroup) throws Exception {
        System.out.println(String.format("Connecting to %s with address %s",
                channelName, multicastGroup));
        this.map = new DistributedMap(channelName, multicastGroup);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("java.net.preferIPv4Stack", "true");
        String channelName = CHANNEL_NAME;
        String multicastGroup = MULTICAST_GROUP;

        if (args.length == 2) {
            channelName = args[0];
            multicastGroup = args[1];
        } else if (args.length == 1) {
            channelName = args[0];
        }
        new Main(channelName, multicastGroup).start();
    }

    public void start() throws InterruptedException {
        Scanner scanner = new Scanner(System.in);
        String line, key, value;
        try {
            while (!(line = scanner.nextLine()).equals("/q")) {
                switch (line.toUpperCase().trim()) {
                    case "PUT":
                        System.out.print("Key:\t");
                        key = scanner.nextLine();
                        System.out.print("Value:\t");
                        value = scanner.nextLine();
                        System.out.println("Response:\t" + map.put(key, value));
                        break;
                    case "CONTAINS":
                        System.out.print("Key:\t");
                        key = scanner.nextLine();
                        System.out.println("Response:\t" + map.containsKey(key));
                        break;
                    case "GET":
                        System.out.print("Key:\t");
                        key = scanner.nextLine();
                        System.out.println("Response:\t" + map.get(key));
                        break;
                    case "REMOVE":
                        System.out.print("Key:\t");
                        key = scanner.nextLine();
                        System.out.println("Response:\t" + map.remove(key));
                        break;
                    case "STATE":
                        map.printState();
                        break;
                    case "CLEAR":
                        map.clear();
                        break;
                    default:
                        System.out.println("Unsupported operation");
                }
            }
        } finally {
            map.close();
        }
    }
}
