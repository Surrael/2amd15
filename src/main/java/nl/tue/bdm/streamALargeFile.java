package nl.tue.bdm;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class streamALargeFile {
  public static void main(String[] args) {
    System.err.println("Starting the application");
    int port = 9999;
    String filePath = "plays.csv";

    try (ServerSocket serverSocket = new ServerSocket(port)) {
      System.out.println("Server is listening on port " + port);
      Socket clientSocket = serverSocket.accept();
      System.out.println("Connected to client");

      try (BufferedReader fileReader = new BufferedReader(new FileReader(filePath));
          PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
        String line;
        while ((line = fileReader.readLine()) != null) {
          out.println(line);
          // Introduce a small delay to simulate streaming
          Thread.sleep(1);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } catch (IOException e) {
      System.err.println(e.getMessage());
    }
  }
}
