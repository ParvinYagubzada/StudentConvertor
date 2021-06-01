import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.Grade;
import model.Student;
import model.Teacher;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Main {

    private static final EntityManagerFactory factory = Persistence.createEntityManagerFactory("studentpu");
    private static final Properties properties = new Properties();
    private static Path targetFolder;
    private static Pattern targetRegex;
    private static Integer threadCount;
    private static Integer lineCount;
    private static Integer readThreadCount;

    static {
        try {
            properties.load(Files.newBufferedReader(Path.of("src/main/resources/app.conf")));
            targetFolder = Path.of(properties.getProperty("target_folder"));
            targetRegex = Pattern.compile(properties.getProperty("file_regex"));
            threadCount = Integer.valueOf(properties.getProperty("reader_thread_count"));
            lineCount = Integer.valueOf(properties.getProperty("read_line_count"));
            readThreadCount = Integer.valueOf(properties.getProperty("read_line_thread_count"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("All")
    public static void main(String[] args) throws InterruptedException {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(threadCount);
        while (true) {
            Path path;
            if ((path = hasData()) != null) {
                service.schedule(() -> {
                    Path rename = path.getParent().getParent().resolve("in_progress").resolve(path.getName(path.getNameCount() - 1));
                    path.toFile().renameTo(rename.toFile());
                    try {
                        fileExecute(rename, lineCount, readThreadCount);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }, 1000, TimeUnit.MILLISECONDS);
            }
            Thread.sleep(2000);
        }
    }

    public static Path hasData() {
        File[] files = targetFolder.toFile().listFiles();
        if (files != null) {
            for (File file : files) {
                if (targetRegex.matcher(file.getName()).matches()) {
                    return Path.of(file.getAbsolutePath());
                }
            }
            return null;
        }
        return null;
    }

    public static void fileExecute(Path path, int lineCount, int threadCount) throws IOException {
        BufferedReader reader = Files.newBufferedReader(path);
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        Stack<String> lines = new Stack<>();
        String line;
        while ((line = reader.readLine()) != null) {
            if (lines.size() == lineCount) {
                Stack<String> temp = lines;
                service.submit(() -> {
                    try {
                        processLines(temp);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                });
                lines = new Stack<>();
            }
            lines.push(line);
        }

        if (lines.size() > 0) {
            processLines(lines);
        }
    }

    public static void processLines(List<String> lines) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        EntityManager localManager = factory.createEntityManager();
        localManager.getTransaction().begin();
        for (String line : lines) {
            Student student = mapper.readValue(line, Student.class);
            for (Teacher teacher: student.getTeachers()) {
                Teacher find = localManager.find(Teacher.class, teacher.getId());
                if (find == null) {
                    localManager.merge(teacher.getSubject());
                    localManager.merge(teacher);
                }

            }
            Grade find = localManager.find(Grade.class, student.getGrade().getId());
            if (find == null)
                localManager.merge(student.getGrade());
            localManager.merge(student);
        }
        localManager.getTransaction().commit();
    }
}
