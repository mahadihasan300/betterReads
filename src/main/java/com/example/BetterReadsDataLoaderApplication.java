package com.example;

import com.example.author.Author;
import com.example.author.AuthorRepository;
import com.example.book.Book;
import com.example.book.BookRepository;
import connection.DataStaxAstraProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsDataLoaderApplication {

    @Autowired
    AuthorRepository authorRepository;

    @Autowired
    BookRepository bookRepository;

    public static void main(String[] args) {
        SpringApplication.run(BetterReadsDataLoaderApplication.class, args);
    }

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocaiton;


    /**
     * Sample Json
     {
     "name": "Alan Hess",
     "personal_name": "Alan Hess",
     "last_modified": {
     "type": "/type/datetime",
     "value": "2008-09-08T03:23:05.850541"
     },
     "key": "/authors/OL93018A",
     "type": {
     "key": "/type/author"
     },
     "revision": 2
     }
     */
    private void initAuthors() {
        Path path = Paths.get(authorDumpLocation);
        try (Stream<String> lines = Files.lines(path)) {
            //lines.limit(10).forEach(line->{
            lines.forEach(line -> {
                // Read and parse the line
                String jsonString = line.substring(line.indexOf("{"));

                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));

                    System.out.println("Saving author " + author.getName());
                    authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Sample Json
     {
     "title": "Le verbe en action. grammaire contrastive des temps verbaux (français, allemand, anglais, espagnol)",
     "covers": [
     3140607
     ],
     "key": "/works/OL10000355W",
     "authors": [
     {
     "type": {
     "key": "/type/author_role"
     },
     "author": {
     "key": "/authors/OL3965376A"
     }
     }
     ],
     "type": {
     "key": "/type/work"
     },
     "subjects": [
     "Comparative and general Grammar",
     "Verb",
     "Tense",
     "French language",
     "German language",
     "English language",
     "Spanish language"
     ],
     "latest_revision": 4,
     "revision": 4,
     "created": {
     "type": "/type/datetime",
     "value": "2009-12-11T01:57:19.964652"
     },
     "last_modified": {
     "type": "/type/datetime",
     "value": "2020-12-07T23:42:52.481900"
     }
     }
     */

    private void initWorks() {
        Path path = Paths.get(worksDumpLocaiton);
        DateTimeFormatter dateFormater =  DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try(Stream<String> lines = Files.lines(path)){

            lines.forEach(line ->{
                String jsonString = line.substring(line.indexOf("{"));
                try{
                    JSONObject jsonObject = new JSONObject(jsonString);
                    Book book = new Book();
                    book.setId(jsonObject.optString("key").replace("/works/",""));
                    book.setName(jsonObject.optString("title"));

                    JSONObject descriptionObject  =  jsonObject.optJSONObject("description");
                    if(descriptionObject !=null){
                        book.setDescription(descriptionObject.optString("value"));
                    }

                    JSONObject publishedObj = jsonObject.optJSONObject("created");
                    if(publishedObj != null){
                        String dateStr = publishedObj.optString("value");
                        book.setPublishedDate(LocalDate.parse(dateStr, dateFormater));
                    }

                    JSONArray coversJsonArr = jsonObject.optJSONArray("covers");
                    if(coversJsonArr != null){
                        List<String> coversIds = new ArrayList<>();
                        for (int i = 0; i < coversJsonArr.length() ; i++){
                            coversIds.add(coversJsonArr.getString(i));
                        }
                        book.setCoverIds(coversIds);
                    }

                    /**
                     "authors": [
                     {
                     "type": {
                     "key": "/type/author_role"
                     },
                     "author": {
                     "key": "/authors/OL3965376A"
                     }
                     }
                     ]
                     */
                    JSONArray authorsJsonArr = jsonObject.optJSONArray("authors");
                    if( authorsJsonArr != null){
                        List<String>  authorIds = new ArrayList<>();
                        for (int i = 0; i < authorsJsonArr.length() ; i++){
                            String authorId =  authorsJsonArr.getJSONObject(i).getJSONObject("author").getString("key")
                                    .replace("/authors/","");
                            authorIds.add(authorId);

                        }
                        book.setAuthorIds(authorIds);

                        List<String> authorNames =  authorIds.stream().map(id -> authorRepository.findById(id))
                                .map(optionalAuthor -> {
                                   if(!optionalAuthor.isPresent()) return "Unknown Author";
                                   return optionalAuthor.get().getName();
                                }).collect(Collectors.toList());
                        book.setAuthorNames(authorNames);

                        System.out.println("Saving Book " + book.getName());
                        bookRepository.save(book);
                    }




                } catch (Exception e) {
                    e.printStackTrace();
                }
            });


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @PostConstruct
    public void start() {
        System.out.println("Application Started.....................");

//		Author author = new Author();
//		author.setId("123");
//		author.setName("Mahadi Hasan");
//		author.setPersonalName("Alfa Wisky");
//
//		authorRepository.save(author);

        System.out.println(worksDumpLocaiton);

        // initAuthors();
        initWorks();
    }

    /**
     * This is necessary to have the Spring Boot app use the Astra secure bundle
     * to connect to the database
     */
    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }

}
