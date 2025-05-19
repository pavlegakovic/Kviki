package rs.netplanet;

import com.google.gson.*;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.*;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.Timer;
import java.util.TimerTask;

public class Main {

    private static final String REQUEST_ID_FILE = "request_id.txt";
    private static int requestIdCounter = loadRequestId(); // Inicijalni request ID

    public static JsonArray convertFileToJson(String filePath) {
        JsonArray jsonArray = new JsonArray();
        String[] fieldNames = {
                "Code", "item_code", "item_name", "Special", "Price",
                "UnitPrice", "QuantityUnit", "Package", "Stock",
                "Category", "Cenovnik", "Vazi_od", "Vazi_do", "StaraCena"
        };

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine(); // Preskoči prvi red (header)
            while ((line = br.readLine()) != null) {
                JsonObject jsonObject = new JsonObject();
                String[] splitParts = line.split("\\|");

                for (int i = 0; i < splitParts.length && i < fieldNames.length; i++) {
                    String value = splitParts[i].trim();

                    jsonObject.addProperty(fieldNames[i], value);
                }

                jsonArray.add(jsonObject);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return jsonArray;
    }

    public static boolean downloadFileIfUpdated(String ftpHost, String ftpUser, String ftpPass, String remoteFilePath, String localFilePath) {
        FTPClient ftpClient = new FTPClient();
        boolean filesDifferent = false;

        try {
            ftpClient.connect(ftpHost);
            ftpClient.login(ftpUser, ftpPass);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            FTPFile[] files = ftpClient.listFiles(remoteFilePath);
            if (files.length == 0) {
                System.out.println("Fajl ne postoji na FTP serveru.");
                return false;
            }
            FTPFile remoteFile = files[0];

            boolean needsDownload = true;
            if (Files.exists(Paths.get(localFilePath))) {
                // Provera da li su lokalni i remote fajlovi identični koristeći MD5 hash
                String localFileHash = getFileChecksum(localFilePath);
                InputStream remoteInputStream = ftpClient.retrieveFileStream(remoteFilePath);
                String remoteFileHash = getInputStreamChecksum(remoteInputStream);

                if (localFileHash.equals(remoteFileHash)) {
                    needsDownload = false;
                } else {
                    filesDifferent = true; // Fajlovi nisu isti po sadržaju
                }

                ftpClient.completePendingCommand(); // Oslobađanje resursa
            } else {
                filesDifferent = true; // Lokalni fajl ne postoji, pa su različiti
            }

            if (needsDownload) {
                try (FileOutputStream fos = new FileOutputStream(localFilePath)) {
                    ftpClient.retrieveFile(remoteFilePath, fos);
                    System.out.println("Fajl je preuzet: " + localFilePath);
                    filesDifferent = true;
                }
            } else {
                System.out.println("Lokalni fajl je azuriran. Nema potrebe za preuzimanjem.");
            }

            ftpClient.logout();
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        } finally {
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.disconnect();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        return filesDifferent;
    }

    public static void sendPostRequest(String urlString, JsonArray res, int requestIdCounter, String scenarioKey, String secret, String storeCodes) {
        try {
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json; utf-8");
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);

            JsonObject mainJsonObject = new JsonObject();
            mainJsonObject.addProperty("scenarioKey", scenarioKey);
            mainJsonObject.addProperty("secret", secret);
            mainJsonObject.addProperty("storeCodes", storeCodes);
            mainJsonObject.addProperty("requestId", requestIdCounter);
            mainJsonObject.add("data", res);

            saveRequestId(requestIdCounter);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = mainJsonObject.toString().getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            try (BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"))) {
                StringBuilder response = new StringBuilder();
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                System.out.println("Response: " + response.toString());
                System.out.println("Request ID: " + requestIdCounter);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int loadRequestId() {
        try (BufferedReader br = new BufferedReader(new FileReader(REQUEST_ID_FILE))) {
            String line = br.readLine();
            if (line != null) {
                return Integer.parseInt(line.trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 1;
    }

    private static void saveRequestId(int requestId) {
        try (FileWriter writer = new FileWriter(REQUEST_ID_FILE)) {
            writer.write(String.valueOf(requestId));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Funkcija za obradu fajla i zamenu vrednosti
    public static void preprocessFile(String filePath) {
        try {
            Path tempFile = Files.createTempFile("processed_", ".csv");

            try (BufferedReader br = new BufferedReader(new FileReader(filePath));
                 BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile.toFile()))) {

                String line;
                while ((line = br.readLine()) != null) {
                    // Zamenjujemo "Posebni cenovnik" i "Osnovni cenovnik" praznim poljem
                    line = line.replace("Posebni cenovnik", "Osnovni cenovnik");
                    bw.write(line);
                    bw.newLine();
                }
            }

            // Premeštamo obrađeni fajl nazad na originalno mesto
            Files.move(tempFile, Paths.get(filePath), StandardCopyOption.REPLACE_EXISTING);
            System.out.println("Fajl je uspešno obrađen.");
        } catch (IOException e) {
            System.err.println("Greška tokom obrade fajla: " + e.getMessage());
        }
    }

    // Funkcija za izračunavanje MD5 hash-a lokalnog fajla
    private static String getFileChecksum(String filePath) throws IOException {
        try (InputStream fis = new FileInputStream(filePath)) {
            return calculateMD5Checksum(fis);
        }
    }

    // Funkcija za izračunavanje MD5 hash-a iz InputStream (remote fajl)
    private static String getInputStreamChecksum(InputStream is) throws IOException {
        return calculateMD5Checksum(is);
    }

    // Generalna funkcija za MD5 hashiranje
    private static String calculateMD5Checksum(InputStream inputStream) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] byteArray = new byte[1024];
            int bytesCount;

            while ((bytesCount = inputStream.read(byteArray)) != -1) {
                digest.update(byteArray, 0, bytesCount);
            }

            byte[] bytes = digest.digest();
            BigInteger no = new BigInteger(1, bytes);
            StringBuilder hashText = new StringBuilder(no.toString(16));

            while (hashText.length() < 32) {
                hashText.insert(0, "0");
            }

            return hashText.toString();
        } catch (Exception e) {
            throw new IOException("Could not calculate MD5 checksum", e);
        }
    }

    public static void main(String[] args) {
        String ftpHost = "78.129.197.54";
        String ftpUser = "kviki@esl.netplanet.rs";
        String ftpPass = "7GybydK2S64yGbZ6BxNa";
        String remoteFilePath = "Item_.txt";

        try {
            String jsonContent = new String(Files.readAllBytes(Paths.get("config.json")));
            JsonArray configArray = JsonParser.parseString(jsonContent).getAsJsonArray();

            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    String localFilePath = "CENE.csv";
                    File file = new File(localFilePath);

                    boolean filesDifferent = downloadFileIfUpdated(ftpHost, ftpUser, ftpPass, remoteFilePath, localFilePath);

                    // Poziv preprocessFile da zameni "Posebni cenovnik" i "Osnovni cenovnik"
                    preprocessFile(localFilePath);

                    JsonArray res = convertFileToJson(file.getAbsolutePath());

                    if (filesDifferent) {
                        // Delimo JSON array na 3 dela
                        int chunkSize = res.size() / 3;

                        // Prvo podelimo u tri dela
                        JsonArray chunk1 = new JsonArray();
                        JsonArray chunk2 = new JsonArray();
                        JsonArray chunk3 = new JsonArray();

                        for (int i = 0; i < chunkSize; i++) {
                            chunk1.add(res.get(i));
                        }
                        for (int i = chunkSize; i < 2 * chunkSize; i++) {
                            chunk2.add(res.get(i));
                        }
                        for (int i = 2 * chunkSize; i < res.size(); i++) {
                            chunk3.add(res.get(i));
                        }

                        // Poslati tri API zahteva sa inkrementiranim request ID
                        for (int i = 0; i < 3; i++) {
                            JsonArray currentChunk = (i == 0) ? chunk1 : (i == 1) ? chunk2 : chunk3;

                            // Povećavamo request ID za svaki poziv
                            requestIdCounter++;
                            for (JsonElement element : configArray) {
                                JsonObject jsonObject = element.getAsJsonObject();
                                String scenarioKey = jsonObject.get("scenarioKey").getAsString();
                                String secret = jsonObject.get("secret").getAsString();
                                String storeCodes = jsonObject.get("storeCodes").getAsString();

                                sendPostRequest("http://49.13.217.99:8085/api/v1/item/sync", currentChunk, requestIdCounter, scenarioKey, secret, storeCodes);
                            }
                        }

                        System.out.println("API zahtevi su uspešno poslati.");
                    } else {
                        System.out.println("Fajlovi su identični, API nije pozvan.");
                    }
                }
            }, 0, 5 * 60 * 1000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
