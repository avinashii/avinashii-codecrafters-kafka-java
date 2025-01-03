package kafka;

import store.Storage;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class KafkaLoader {

    private final DataInputStream inputStream;
    private final Storage storage;

    public KafkaLoader(DataInputStream inputStream, Storage storage) {
        this.inputStream = inputStream;
        this.storage = storage;
    }

    public void load() throws IOException {

        byte[] serverProperties = inputStream.readAllBytes();
        String serverContents = new String(serverProperties);

        final var logDirs = "log.dirs=";
        final var directoryIndex = serverContents.indexOf(logDirs);
        final var directoryPath = serverContents.substring(directoryIndex + logDirs.length());

        System.out.println(serverContents+" "+directoryIndex+" "+directoryPath);

    }

    public static void load(Path path, Storage storage) throws IOException {
        try (final var fileInputStream = Files.newInputStream(path)) {
            load(fileInputStream, storage);
        }
    }

    public static void load(InputStream inputStream, Storage storage) throws IOException {
        try (final var dataInputStream = new DataInputStream(inputStream)) {
            final var loader = new KafkaLoader(dataInputStream, storage);
            loader.load();
        }
    }
}
