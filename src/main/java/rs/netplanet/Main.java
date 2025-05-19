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

    public static JsonArray convertFileToJson(String filePath) {
        JsonArray jsonArray = new JsonArray();
        String[] fieldNames = {
                "Code", "item_code", "item_name", "Special", "Price",
                "UnitPrice", "QuantityUnit", "Package", "Stock",
                "Category", "Cenovnik", "Vazi_od", "Vazi_do", "StaraCena"
        };

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine(); // Preskoči header
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

            boolean needsDownload = true;
            if (Files.exists(Paths.get(localFilePath))) {
                String localHash = getFileChecksum(localFilePath);
                InputStream remoteInputStream = ftpClient.retrieveFileStream(remoteFilePath);
                String remoteHash = getInputStreamChecksum(remoteInputStream);

                if (localHash.equals(remoteHash)) {
                    needsDownload = false;
                } else {
                    filesDifferent = true;
                }

                ftpClient.completePendingCommand();
            } else {
                filesDifferent = true;
            }

            if (needsDownload) {
                try (FileOutputStream fos = new FileOutputStream(localFilePath)) {
                    ftpClient.retrieveFile(remoteFilePath, fos);
                    System.out.println("Fajl je preuzet: " + localFilePath);
                    filesDifferent = true;
                }
            } else {
                System.out.println("Lokalni fajl je ažuriran. Nema potrebe za preuzimanjem.");
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

    public static void sendPostRequest(String urlString, JsonArray res, long requestId, String scenarioKey, String secret, String storeCodes) {
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
            mainJsonObject.addProperty("requestId", requestId);
            mainJsonObject.add("data", res);

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
                System.out.println("Request ID (timestamp): " + requestId);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void preprocessFile(String filePath) {
        try {
            Path tempFile = Files.createTempFile("processed_", ".csv");

            try (BufferedReader br = new BufferedReader(new FileReader(filePath));
                 BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile.toFile()))) {

                String line;
                while ((line = br.readLine()) != null) {
                    line = line.replace("Posebni cenovnik", "Osnovni cenovnik");
                    bw.write(line);
                    bw.newLine();
                }
            }

            Files.move(tempFile, Paths.get(filePath), StandardCopyOption.REPLACE_EXISTING);
            System.out.println("Fajl je uspešno obrađen.");
        } catch (IOException e) {
            System.err.println("Greška tokom obrade fajla: " + e.getMessage());
        }
    }

    private static String getFileChecksum(String filePath) throws IOException {
        try (InputStream fis = new FileInputStream(filePath)) {
            return calculateMD5Checksum(fis);
        }
    }

    private static String getInputStreamChecksum(InputStream is) throws IOException {
        return calculateMD5Checksum(is);
    }

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

                    preprocessFile(localFilePath);

                    JsonArray res = convertFileToJson(file.getAbsolutePath());

                    if (filesDifferent) {
                        int chunkSize = res.size() / 3;
                        JsonArray chunk1 = new JsonArray();
                        JsonArray chunk2 = new JsonArray();
                        JsonArray chunk3 = new JsonArray();

                        for (int i = 0; i < chunkSize; i++) chunk1.add(res.get(i));
                        for (int i = chunkSize; i < 2 * chunkSize; i++) chunk2.add(res.get(i));
                        for (int i = 2 * chunkSize; i < res.size(); i++) chunk3.add(res.get(i));

                        JsonArray[] chunks = {chunk1, chunk2, chunk3};

                        for (JsonArray currentChunk : chunks) {
                            long timestampRequestId = System.currentTimeMillis();

                            for (JsonElement element : configArray) {
                                JsonObject jsonObject = element.getAsJsonObject();
                                String scenarioKey = jsonObject.get("scenarioKey").getAsString();
                                String secret = jsonObject.get("secret").getAsString();
                                String storeCodes = jsonObject.get("storeCodes").getAsString();

                                sendPostRequest("http://49.13.217.99:8085/api/v1/item/sync", currentChunk, timestampRequestId, scenarioKey, secret, storeCodes);
                            }

                            try {
                                Thread.sleep(5); // da ne budu identični timestamp-ovi
                            } catch (InterruptedException ignored) {
                            }
                        }

                        System.out.println("API zahtevi su uspešno poslati.");
                    } else {
                        System.out.println("Fajlovi su identični, API nije pozvan.");
                    }
                }
            }, 0, 5 * 60 * 1000); // svakih 5 minuta

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
