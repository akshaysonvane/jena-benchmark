package com.marklogic.rdfbench;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.Transaction;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

class QuadLoader {

    private DatabaseClient client() {
        return DatabaseClientFactory.newClient("localhost", 8000, new DatabaseClientFactory.DigestAuthContext("admin", "admin"));
    }

    private ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(24);

    private List<Future<?>> futures = new ArrayList<>();

    private Transaction tx;
    private XMLDocumentManager documentManager;
    private DocumentWriteSet writeSet;


    public void parseQuadsAndLoad() throws Exception {
        DatabaseClient client = client();

        InputStream input = new FileInputStream("data/nquads/Curriculum_Largest.nq");
//        InputStream input = new FileInputStream("data/nquads/nquads1.nq");
//        InputStream input = new FileInputStream("data/turtletriples/turtle600k.ttl");



        RDFParser parser = Rio.createParser(RDFFormat.NQUADS, SimpleValueFactory.getInstance());
        parser.setRDFHandler(new RDFHandler() {

            int i = 0;
            int j = 0;
            int T_PER_DOC = 100;
            int DOCS_PER_BATCH = 3000;
            int n = 0;

            public void startDoc() {
                startDoc(graphCache.get("default"));
            }

            public void startDoc(StringBuilder sb) {
                sb.append("<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">\n");
            }

            public void endDoc() {
                endDoc("default");
            }

            public void endDoc(String graph) {
                StringBuilder sb = graphCache.get(graph);
                sb.append("</sem:triples>\n");

                String st = sb.toString();
                if (graph != null) {
                    DocumentMetadataHandle metadata = new DocumentMetadataHandle().withCollections(graph);
                    writeSet.add("/" + j++ + ".xml", metadata, new StringHandle(st));
                } else {
                    writeSet.add("/" + j++ + ".xml", new StringHandle(st));
                }

                n++;
                if (n == DOCS_PER_BATCH) {
                    n = 0;
                    futures.add(executor.submit(new QWTask(writeSet, tx, documentManager)));
                    writeSet = documentManager.newWriteSet();
                }

                graphCache.remove(graph);
            }


            Map<String, StringBuilder> graphCache;
            Map<String, Integer> tripleCounts;

            @Override
            public void startRDF() throws RDFHandlerException {
                tx = client.openTransaction();
                documentManager = client.newXMLDocumentManager();
                graphCache = new ConcurrentHashMap<>();
                graphCache.put("default", new StringBuilder());
                startDoc();
                tripleCounts = new ConcurrentHashMap<>();
                writeSet = documentManager.newWriteSet();
            }

            @Override
            public void endRDF() throws RDFHandlerException {

                for (String key : graphCache.keySet()) {
                    endDoc(key);
                }

                futures.add(executor.submit(new WTask(writeSet, tx, documentManager)));
            }

            @Override
            public void handleNamespace(String prefix, String uri) throws RDFHandlerException {

            }

            @Override
            public void handleStatement(Statement st) throws RDFHandlerException {
                if (st.getContext() != null) {
                    //Quad
                    String graph = st.getContext().toString();

                    if (tripleCounts.containsKey(graph)) {
                        int j = 1 + tripleCounts.get(graph);
                        tripleCounts.put(graph, j);
                        if (j == T_PER_DOC) {
                            tripleCounts.put(graph, 0);
                            endDoc(graph);
                            graphCache.put(graph, new StringBuilder());
                            startDoc(graphCache.get(graph));
                        }
                    } else {
                        tripleCounts.put(graph, 1);
                        graphCache.put(graph, new StringBuilder());
                        startDoc(graphCache.get(graph));
                    }

                    triple(graphCache.get(graph), st);
                } else {
                    //Triple
                    i++;
                    if (i == T_PER_DOC) {
                        i = 0;
                        endDoc();
                        StringBuilder sb = new StringBuilder();
                        graphCache.put("default", sb);
                        startDoc(graphCache.get("default"));
                    }

                    triple(graphCache.get("default"), st);
                }
            }

            @Override
            public void handleComment(String comment) throws RDFHandlerException {

            }

            private void triple(StringBuilder sb, Statement st) {
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
        });

        parser.parse(input, "http://example.org");

        for (Future<?> future : futures) {
            if(future.get() != null) {
                tx.rollback();
                break;
            }
        }

        executor.shutdown();

        if (executor.getActiveCount() == 0) {
            tx.commit();
        }
    }
}

class QWTask implements Runnable {
    private DocumentWriteSet writeSet;
    private Transaction tx;
    private XMLDocumentManager documentManager;

    QWTask(DocumentWriteSet writeSet, Transaction tx, XMLDocumentManager documentManager) {
        this.writeSet = writeSet;
        this.tx = tx;
        this.documentManager = documentManager;
    }

    @Override
    public void run() {
        documentManager.write(writeSet, tx);
    }
}

public class MultiThreadedClientSideParseQuads {
    public static void main(String[] args) {
        QuadLoader quadLoader = new QuadLoader();
        try {
            long start = System.currentTimeMillis();
            quadLoader.parseQuadsAndLoad();
            System.out.println("Time since load began: " + (System.currentTimeMillis() - start) / 1000 + " seconds");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
