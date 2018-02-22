package com.marklogic.rdfbench;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.Transaction;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.StringHandle;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

class Loader {

    private DatabaseClient client() {
        return DatabaseClientFactory.newClient("localhost", 8000, new DatabaseClientFactory.DigestAuthContext("admin", "admin"));
    }

    private ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(18);

    private List<Future<?>> futures = new ArrayList<>();

    private Transaction tx;
    private XMLDocumentManager documentManager;


    public void parseTurtleAndLoadRDF4J() throws Exception {
        DatabaseClient client = client();

        InputStream input = new FileInputStream("data/turtletriples/turtle600k.ttl");

        RDFParser parser = Rio.createParser(RDFFormat.TURTLE, SimpleValueFactory.getInstance());
        parser.setRDFHandler(new RDFHandler() {
            StringBuffer sb;
            int i = 0;
            int j = 0;
            int T_PER_DOC = 2000;

            void startDoc() {
                sb = new StringBuffer();
                sb.append("<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">\n");
            }

            void endDoc() {
                sb.append("</sem:triples>\n");
                futures.add(executor.submit(new Task(j++, sb.toString(), tx, documentManager)));
            }

            @Override
            public void startRDF() throws RDFHandlerException {
                tx = client.openTransaction();
                documentManager = client.newXMLDocumentManager();
                startDoc();
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                endDoc();
            }

            @Override
            public void handleNamespace(String prefix, String uri) throws RDFHandlerException {

            }

            @Override
            public void handleStatement(Statement st) throws RDFHandlerException {
                i++;
                if (i == T_PER_DOC) {
                    i = 0;
                    endDoc();
                    startDoc();
                }

                sb.append("<sem:triple>\n");
                sb.append("<sem:subject>").append(st.getSubject()).append("</sem:subject>\n");
                sb.append("<sem:predicate>").append(st.getPredicate()).append("</sem:predicate>\n");
                Value object = st.getObject();
                if (object instanceof Literal) {
                    Literal lit = (Literal) object;
                    sb.append("<sem:object datatype=\"").append(lit.getDatatype()).append("\">").append(object.stringValue()).append("</sem:object>\n");
                } else {
                    sb.append("<sem:object>").append(object.stringValue()).append("</sem:object>\n");
                }
                sb.append("</sem:triple>\n");
            }

            @Override
            public void handleComment(String comment) throws RDFHandlerException {

            }
        });

        parser.parse(input, "http://example.org");

        for (Future<?> future : futures) {
            if(future.get() != null) {
                tx.rollback();
                break;
            }
        }

        if (executor.getActiveCount() == 0) {
            executor.shutdown();
            tx.commit();
        }
    }
}

class Task implements Runnable
{
    private int j;
    private String string;
    private Transaction tx;
    private XMLDocumentManager documentManager;

    Task(int j, String string, Transaction tx, XMLDocumentManager documentManager)
    {
        this.j = j;
        this.string = string;
        this.tx = tx;
        this.documentManager = documentManager;
    }
    @Override
    public void run() {
        documentManager.write("/"+ j++ + ".xml", new StringHandle(string), tx);
    }
}

public class MultiThreadedClientSideParse {
    public static void main(String[] args) {
        Loader loader = new Loader();
        try {
            long start = System.currentTimeMillis();
            loader.parseTurtleAndLoadRDF4J();
            System.out.println("Time since load began: " + (System.currentTimeMillis() - start) / 1000 + " seconds");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}