package map;

import org.jgroups.*;
import org.jgroups.Message;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;

public class DistributedMap extends ReceiverAdapter implements SimpleStringMap {
    private final HashMap<String, String> hashMap = new HashMap<>();
    private JChannel channel;

    public DistributedMap(JChannel channel) throws Exception {
        this.channel = channel;
        channel.setReceiver(this);
        channel.getState(null, 30000);
    }

    public DistributedMap(String channelName, String multicastGroup) throws Exception {
        channel = new JChannel();
        channel.setReceiver(this);
        ProtocolStack stack = new ProtocolStack();
        channel.setProtocolStack(stack);
        prepareProtocolStack(stack, multicastGroup);
        channel.connect(channelName);
        System.out.println("Acquiring state");
        channel.getState(null, 30000);
        System.out.println("State acquired");
    }

    private static void prepareProtocolStack(ProtocolStack stack, String multicastGroup) throws Exception {
        stack.addProtocol(new UDP().setValue("mcast_group_addr",InetAddress.getByName(multicastGroup)))
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
                .addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
                .addProtocol(new STATE_TRANSFER());

        stack.init();
    }

    public void viewAccepted(View new_view) {
        if (new_view instanceof MergeView) {
            ViewHandler handler = new ViewHandler(channel, (MergeView) new_view);
            handler.start();
        }
    }


    private static class ViewHandler extends Thread {
        JChannel ch;
        MergeView view;

        private ViewHandler(JChannel ch, MergeView view) {
            this.ch = ch;
            this.view = view;
        }

        public void run() {
            List<View> subgroups = view.getSubgroups();
            View tmp_view = subgroups.get(0);
            Address local_addr = ch.getAddress();
            if (local_addr.equals(tmp_view.getCoord())) {
                System.out.println("Merge request");
                Address state_address = subgroups
                        .get((int) Math.floor(Math.random() * subgroups.size()))
                        .getCoord();
                System.out.println("Address of new state: " + state_address);
                MapMessage message = new MapMessage()
                        .setOperation(Operation.SET_STATE)
                        .setStateAddress(state_address);
                try {
                    ch.send(null, message);
                    ch.getState(state_address, 30000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void receive(Message msg) {
        if (channel.getAddress().equals(msg.getSrc())) {
            return;
        }
        if (!(msg.getObject() instanceof  MapMessage)) {
            return;
        }
        MapMessage mapMessage = (MapMessage) msg.getObject();
        switch (mapMessage.getOperation()) {
            case PUT:
                System.out.println("Received message PUT");
                this.hashMap.put(mapMessage.getKey(), mapMessage.getValue());
                break;
            case REMOVE:
                System.out.println("Received message REMOVE");
                this.hashMap.remove(mapMessage.getKey());
                break;
            case SET_STATE:
                System.out.println("Received message SET_STATE");
                if (channel.getAddress().equals(mapMessage.getStateAddress())) {
                    break;
                }
                System.out.println("Acquiring state from " + mapMessage.getStateAddress());
                new Thread(() -> {
                    try {
                        channel.getState(mapMessage.getStateAddress(), 30000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).start();
                break;
            default:
                break;
        }
    }

    @Override
    public void getState(OutputStream output) throws Exception {
        synchronized (hashMap) {
            Util.objectToStream(hashMap, new ObjectOutputStream(output));
        }
    }

    @Override
    public void setState(InputStream input) throws Exception {
        synchronized (hashMap) {
            HashMap<String, String> map;
            map = (HashMap<String, String>) Util.objectFromStream(new ObjectInputStream(input));
            hashMap.clear();
            hashMap.putAll(map);
        }
    }

    @Override
    public boolean containsKey(String key) {
        synchronized (hashMap) {
            return hashMap.containsKey(key);
        }
    }

    @Override
    public String get(String key) {
        synchronized (hashMap) {
            return hashMap.get(key);
        }
    }

    @Override
    public String put(String key, String value) {
        synchronized (hashMap) {
            MapMessage message = new MapMessage()
                    .setOperation(Operation.PUT)
                    .setKey(key)
                    .setValue(value);
            try {
                channel.send(null, message);
            } catch (Exception e) {
                System.out.println("Something went wrong while putting");
                e.printStackTrace();
            }
            return hashMap.put(key, value);
        }
    }

    @Override
    public String remove(String key) {
        synchronized (hashMap) {
            MapMessage message = new MapMessage()
                    .setOperation(Operation.REMOVE)
                    .setKey(key);
            try {
                channel.send(null, message);
            } catch (Exception e) {
                System.out.println("Something went wrong while removing");
                e.printStackTrace();
            }
            return hashMap.remove(key);
        }
    }

    public void printState() {
        System.out.println("Key\tValue");
        hashMap.forEach((key, value) -> {
            System.out.println(String.format("%s\t%s", key, value));
        });
    }

    public void close() throws InterruptedException {
        channel.close();
        Thread.sleep(5000);
    }

    public void clear() {
        synchronized (hashMap) {
            MapMessage message = new MapMessage()
                    .setOperation(Operation.REMOVE);
            hashMap.keySet().forEach(key -> {
                message.setKey(key);
                try {
                    channel.send(null, message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            hashMap.clear();
        }
    }
}
