import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;


public abstract class AbstractDude extends Thread implements Receiver, Sender {
		/**client socket used for data communication*/
		protected Socket dudeSocket;
		protected ObjectInputStream reader;
		protected ObjectOutputStream writer;
		
		public AbstractDude (Socket s) throws IOException {
			if (s != null)
				initIO(s);
		}
		
		public void initIO(Socket s) throws IOException {
			// TODO Auto-generated method stub
			dudeSocket = s;
			writer = new ObjectOutputStream(dudeSocket.getOutputStream());
			writer.flush();
			reader = new ObjectInputStream(dudeSocket.getInputStream());
		}

		public void closeSocket() throws IOException {
			reader.close();
			writer.close();
			dudeSocket.close();
		}

		@Override
		public abstract Message generateMsg(String type, Object content);

		@Override
		public void send(Message msg) throws IOException {
			// TODO Auto-generated method stub
			writer.writeObject(msg);
			writer.flush();
		}

		@Override
		public Message receive() throws IOException {
			try {
				return (Message) reader.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}

		@Override
		public abstract Object process(Message msg);
	}
