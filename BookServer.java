import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;

public class BookServer
{
    public record checkoutCollection(String bookTitle, String userName) {};

    public static class BookLibrary
    {
        private final ReentrantLock libraryBooksLock = new ReentrantLock();
        Map<String, Integer> libraryBooks = new HashMap<>();
        private final ReentrantLock checkedoutBooksLock = new ReentrantLock();
        Map<Integer, checkoutCollection> checkedoutBooks = new HashMap<>();
        int nextLoanID = 1;

        String handleRequestString(String input)
        {
                String[] command = input.split("#");

                if(command.length < 1) { return "Error#Invalid Command"; }
                switch(command[0])
                {
                    case "set-mode":
                    {
                        if(command.length != 2) { return "Error#set-mode input incorrect param length"; }
                        if(command[1].equalsIgnoreCase("t")) { return "Success#The communication mode is set to TCP"; }
                        else if(command[1].equals("u")) { return "Success#The communication mode is set to UDP"; }
                        
                        return "Error#Incorrect second line command in set-mode";
                    }
                    case "begin-loan":
                    {
                        if(command.length != 3) { return "Error#begin-loan input incorrect param length"; }

                        libraryBooksLock.lock();
                        checkedoutBooksLock.lock();
                        Integer LoanID = 0;
                        try 
                        {
                            if(!libraryBooks.containsKey(command[2])) { return "Error#Request Failed - We do not have this book"; }

                            Integer bookCount = libraryBooks.get(command[2]);
                            if(bookCount <= 0) { return "Error#Request Failed - Book not available"; }

                            libraryBooks.put(command[2], bookCount - 1);
                            LoanID = nextLoanID++;
                            checkedoutBooks.put(LoanID, new checkoutCollection(command[2], command[1]));
                        } 
                        finally
                        {
                            libraryBooksLock.unlock();
                            checkedoutBooksLock.unlock();
                        }

                        return "Success#Your request has been approved, " + LoanID.toString() + " " + command[1] + " " + command[2];
                    }
                    case "end-loan":
                    {
                        if(command.length != 2) { return "Error#end-loan input incorrect param length"; }

                        libraryBooksLock.lock();
                        checkedoutBooksLock.lock();
                        Integer loanID = Integer.parseInt(command[1]);

                        try 
                        {
                            
                            if(!checkedoutBooks.containsKey(loanID)) { return "Error#" + loanID.toString() + " not found, no such borrow record"; }

                            checkoutCollection checkout = checkedoutBooks.get(loanID);
                            int bookCount = libraryBooks.get(checkout.bookTitle);

                            libraryBooks.put(checkout.bookTitle, bookCount + 1);
                            checkedoutBooks.remove(loanID);
                        } 
                        finally
                        {
                            libraryBooksLock.unlock();
                            checkedoutBooksLock.unlock();
                        }

                        return "Success#" + loanID.toString() + " is returned";
                    }
                    case "get-loans":
                    {
                        if(command.length != 2) { return "Error#get-loans input incorrect param length"; }

                        String returnLoans = "";
                        checkedoutBooksLock.lock();

                        try 
                        {
                            for (Map.Entry<Integer, checkoutCollection> entry : checkedoutBooks.entrySet())
                            {
                                if(entry.getValue().userName.equalsIgnoreCase(command[1]))
                                {
                                    returnLoans += "#" + entry.getKey().toString() + " " + entry.getValue().bookTitle;
                                }
                            }
                        } 
                        finally
                        {
                            checkedoutBooksLock.unlock();
                        }

                        if(returnLoans == "") { return "Error#No record found for " + command[1]; }
                            
                        return "Success" + returnLoans;
                    }
                    case "get-inventory":
                    {
                        if(command.length != 1) { return "Error#get-inventory input incorrect param length"; }

                        String returnBooks = "";
                        libraryBooksLock.lock();

                        try 
                        {
                            for (Map.Entry<String, Integer> entry : libraryBooks.entrySet())
                            {
                                returnBooks += "#" + entry.getKey() + " " + entry.getValue().toString();
                            }
                        } 
                        finally
                        {
                            libraryBooksLock.unlock();
                        }
                            
                        return "Success" + returnBooks;
                    }
                    case "exit":
                    {
                        if(command.length != 1) { return "Error#exit input incorrect param length"; }

                        libraryBooksLock.lock();

                        try 
                        {
                            try (BufferedWriter writer = new BufferedWriter(new FileWriter("inventory.txt")))
                            {
                                for (Map.Entry<String, Integer> entry : libraryBooks.entrySet())
                                {
                                    writer.write('"' + entry.getKey() + "\" " + entry.getValue().toString());
                                    writer.newLine();
                                }
                            } 
                            catch (IOException e)
                            {
                                e.printStackTrace();
                            }
                        } 
                        finally
                        {
                            libraryBooksLock.unlock();
                        }
                            
                        return "Success";
                    }
                }

                return "Error#No matching command found";
        }
    }

    private static void handleUDPClient(DatagramPacket incomingPacket, DatagramSocket udpSocket, BookLibrary library)
    {
        String clientMessage = new String(incomingPacket.getData(), 0, incomingPacket.getLength());
        System.out.println("[UDP Received]: " + clientMessage);

        String responseMessage = library.handleRequestString(clientMessage);
        byte[] responseData = responseMessage.getBytes();
        
        InetAddress clientAddress = incomingPacket.getAddress();
        int clientPort = incomingPacket.getPort();
        
        DatagramPacket replyPacket = new DatagramPacket(responseData, responseData.length, clientAddress, clientPort);
        try 
        {
            udpSocket.send(replyPacket);
        } 
        catch (IOException e)
        {
            System.out.println("Failed to Send: " + e.getMessage());
        }
        
    }

    private static void UDPServer(int port, BookLibrary library) 
    {
        try (DatagramSocket udpSocket = new DatagramSocket(port))
        { // Create UDP Socket
            System.out.println("UDP Server listening on port " + port);
            byte[] buffer = new byte[1024];

            while (true) 
            {
                DatagramPacket incomingPacket = new DatagramPacket(buffer, buffer.length);
                
                udpSocket.receive(incomingPacket); 
                
                new Thread(() -> handleUDPClient(incomingPacket, udpSocket, library)).start();
            }
        } 
        catch (IOException e)
        {
            System.err.println("UDP Server Exception: " + e.getMessage());
        }
        
    }

    private static void handleTcpClient(Socket clientSocket, BookLibrary library)
    {
        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
        ) 
        {
            String clientMessage;
            while ((clientMessage = reader.readLine()) != null)
            {
                System.out.println("[TCP Received]: " + clientMessage);
                writer.println(library.handleRequestString(clientMessage));
            }
        } 
        catch (IOException e)
        {
            System.err.println("Error handling TCP client: " + e.getMessage());
        } 
        finally
        {
            try
            {
                clientSocket.close();
            } 
            catch (IOException e)
            {
                System.err.println("Failed to close TCP client socket: " + e.getMessage());
            }
        }
    }

    private static void TCPServer(int port, BookLibrary library) 
    {
        try (ServerSocket serverSocket = new ServerSocket(port)) 
        { 
            System.out.println("TCP Server listening on port " + port);

            while (true)
            {
                Socket clientSocket = serverSocket.accept(); 
                System.out.println("New TCP Client connected: " + clientSocket.getRemoteSocketAddress());
                
                new Thread(() -> handleTcpClient(clientSocket, library)).start();
            }
        } 
        catch (IOException e)
        {
            System.err.println("TCP Server Exception: " + e.getMessage());
        }
    }

    public static void main(String[] args)
    {
        int tcpPort;
        int udpPort;
        if (args.length != 1) {
            System.out.println("ERROR: Provide 1 argument: input file containing initial inventory");
            System.exit(-1);
        }
        String fileName = args[0];
        tcpPort = 7000;
        udpPort = 8000;
        BookLibrary library = new BookLibrary();

        // parse the inventory file
        Path filePath = Paths.get(fileName);

        try (BufferedReader reader = Files.newBufferedReader(filePath))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] command = line.split("\"");
                library.libraryBooks.put(command[1], Integer.parseInt(command[2].substring(1)));
            }
            
        }
        catch (IOException e)
        {
            System.err.println("Error reading the file: " + e.getMessage());
        }

        //handle request from clients
        Thread TCPThread = new Thread(() -> TCPServer(tcpPort, library));
        Thread UDPThread = new Thread(() -> UDPServer(udpPort, library));

        TCPThread.start();
        UDPThread.start();

        try 
        {
            TCPThread.join();
            UDPThread.join();
        } 
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private static void TestClient(int port, String file_name)
    {
        try (Socket socket = new Socket("localhost", port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            System.out.println("[CLIENT] Connected to server successfully!");
            List<String> list = List.of(
                "set-mode#t",
                "begin-loan#Mike#The Letter",
                "begin-loan#Mike#Divergent",
                "get-loans#Mike",
                "get-inventory",
                "exit"
            );

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file_name)))
            {
                for(String massageToSend : list)
                {
                    out.println(massageToSend);
                    writer.write("[CLIENT] Sent: " + massageToSend);
                    writer.newLine();

                    // Read the server's response
                    String serverResponse = in.readLine();
                    writer.write("[CLIENT] Received from server: " + serverResponse);
                    writer.newLine();
                }
            } 
            catch (IOException e)
            {
                e.printStackTrace();
            }
            

        } 
        catch (Exception e)
        {
            System.err.println("[CLIENT] Error: " + e.getMessage());
        }
    }
}
