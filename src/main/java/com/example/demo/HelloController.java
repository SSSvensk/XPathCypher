package com.example.demo;

import java.util.*;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;

import org.springframework.jdbc.core.JdbcTemplate;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Query;

import static org.neo4j.driver.Values.parameters;

@RestController
public class HelloController {

	@Value("${spring.neo4j.uri}")
    private String neo4jUri;

	@Value("${spring.neo4j.authentication.username}")
    private String neo4jUser;

	@Value("${spring.neo4j.authentication.password}")
    private String neo4jPassword;

	@Autowired
    private JdbcTemplate jdbcTemplate;

	@GetMapping("/")
	public String index() {
		return "Greetings from Spring Boot!";
	}

	@GetMapping("/translate")
	public JSONObject translateXPath(@RequestParam("query") String query) {

		try {

			System.out.println(neo4jUri);
			System.out.println(neo4jUser);
			System.out.println(neo4jPassword);
            
            // create a CharStream that reads from standard input
            ANTLRInputStream input = new ANTLRInputStream(query);

			System.out.println(query);

            // create a lexer that feeds off of input CharStream
            xpathLexer lexer = new xpathLexer(input);

            // create a buffer of tokens pulled from the lexer
            CommonTokenStream tokens = new CommonTokenStream(lexer);

            // create a parser that feeds off the tokens buffer
            xpathParser parser = new xpathParser(tokens);

			StringBuilder sb = new StringBuilder();

			XPathCypherListener mylistener = new XPathCypherListener();
			parser.addParseListener(mylistener);
            ParseTree tree = parser.main();    // begin parsing at rule main
            
            final String cypQuery = sb.append(mylistener.getQuery()).toString();
			JSONObject mainObject = new JSONObject();
			mainObject.put("query", cypQuery);
			System.out.println(cypQuery);
			return mainObject;
            
    	} catch (Exception e) {
    		System.out.println("virhe!");
    		System.out.println(e);
			return new JSONObject();
    	}
	}

	@GetMapping("/execute")
	public List<JSONObject> test(@RequestParam("query") String query) {

		final List<JSONObject> names = new ArrayList<>();

		try {
            
            // create a CharStream that reads from standard input
            ANTLRInputStream input = new ANTLRInputStream(query);

			System.out.println(query);

            // create a lexer that feeds off of input CharStream
            xpathLexer lexer = new xpathLexer(input);

            // create a buffer of tokens pulled from the lexer
            CommonTokenStream tokens = new CommonTokenStream(lexer);

            // create a parser that feeds off the tokens buffer
            xpathParser parser = new xpathParser(tokens);

			StringBuilder sb = new StringBuilder();
            String cypQ = "";

				XPathCypherListener mylistener = new XPathCypherListener();
				parser.addParseListener(mylistener);
                ParseTree tree = parser.main();    // begin parsing at rule main
            
                final String cypQuery = sb.append(mylistener.getQuery()).toString();
				System.out.println(cypQ);

				//"bolt://localhost:7687"
				//"neo4j"
				//"password"

				Driver driver = GraphDatabase.driver(neo4jUri, AuthTokens.basic(neo4jUser, neo4jPassword));
				
				try (var session = driver.session()) {
                    return session.executeRead(tx -> {
						final List<JSONObject> values = new ArrayList<>();
                        var result = tx.run(cypQuery);

                        while (result.hasNext()) {
							Record r = result.next();
							JSONObject mainObject = new JSONObject();
							JSONObject properties = new JSONObject();

							for (int i = 0; i < r.values().size(); i++) {

								Map<String, Object> res = new HashMap();

								System.out.println(r.values().get(i).type().name());

								if (r.values().get(i).type().name().equals("NODE")) {
									res = r.values().get(i).asNode().asMap();
									mainObject.put("labels", r.values().get(i).asNode().labels());
								} else if (r.values().get(i).type().name().equals("RELATIONSHIP")) {
								    res = r.values().get(i).asRelationship().asMap();
									mainObject.put("end", r.values().get(i).asRelationship().endNodeElementId());
									mainObject.put("start", r.values().get(i).asRelationship().startNodeElementId());
									mainObject.put("type", r.values().get(i).asRelationship().type());
								} else if (r.values().get(i).type().name().equals("INTEGER")) {
									mainObject.put("result", r.values().get(i).asInt());
								} else if (r.values().get(i).type().name().equals("STRING")) {
									mainObject.put("result", r.values().get(i).asString());
								}

								if (r.values().get(i).type().name().equals("NODE") || r.values().get(i).type().name().equals("RELATIONSHIP")) {
									for (Map.Entry<String, Object> entry : res.entrySet()) {
                                        properties.put(entry.getKey(), entry.getValue());
                                    }
									mainObject.put("properties", properties);
								}
							}
                            values.add(mainObject);
                        }
                        return values;
                    });
                }
            
    	} catch (Exception e) {
    		System.out.println("virhe!");
    		System.out.println(e);
			return names;
    	}
	}
}