package com.marklogic.rdfbench;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.Transaction;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.*;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.client.JenaDatabaseClient;
import com.marklogic.semantics.rdf4j.MarkLogicRepository;
import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import com.marklogic.xcc.*;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLUtil;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.openjdk.jmh.annotations.Benchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class XMLTripleLoader {

    private static Logger logger = LoggerFactory.getLogger(XMLTripleLoader.class);
    //private static String HOST = "localhost";
    private static String HOST = "rh7-intel64-80-qa-dev-5";


    // this is a streaming triple-to-xml serializer
    private StreamRDF sink(WriteBatcher batcher) {
        return new StreamRDF() {
            int i = 0;
            int j = 0;
            int T_PER_DOC = 100;


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
                //System.out.println(sb.toString());
                if (j % 500 == 0) logger.debug("Added 500 documents");
                if (graph != null) {
                    DocumentMetadataHandle metadata = new DocumentMetadataHandle().withCollections(graph);
                    batcher.add("/" + j++ + ".xml", metadata, new StringHandle(sb.toString()).withFormat(Format.XML));
                } else {
                    batcher.add("/" + j++ + ".xml", new StringHandle(sb.toString()).withFormat(Format.XML));
                }
                graphCache.remove(graph);
                //try {
                    //Thread.sleep(15);  // avoid starting multiple transactions
                //} catch (InterruptedException e) {
                    //e.printStackTrace();
                //}
            }


            Map<String, StringBuilder> graphCache;
            Map<String, Integer> tripleCounts;

            @Override
            public void start() {
                graphCache = new ConcurrentHashMap<String,StringBuilder>();
                graphCache.put("default", new StringBuilder());
                startDoc();
                tripleCounts = new ConcurrentHashMap<String,Integer>();
            }

            @Override
            public void triple(Triple triple) {
                i++;
                if (i == T_PER_DOC) {
                    i = 0;
                    endDoc();
                    StringBuilder sb = new StringBuilder();
                    graphCache.put("default", sb);
                    startDoc(graphCache.get("default"));
                }

                triple(graphCache.get("default"), triple);
            }

            private void triple(StringBuilder sb, Triple triple) {
                //logger.debug("I got a triple");
                sb.append("<sem:triple>\n");
                sb.append("<sem:subject>" + triple.getSubject().getURI() + "</sem:subject>\n");
                sb.append("<sem:predicate>" + triple.getPredicate().getURI() + "</sem:predicate>\n");
                if (triple.getObject().isLiteral()) {
                    sb.append("<sem:object datatype=\"" + triple.getObject().getLiteralDatatype().getURI() + "\">" + triple.getObject().getLiteralLexicalForm() + "</sem:object>\n");
                } else {
                    sb.append("<sem:object>" + triple.getObject().getURI() + "</sem:object>\n");
                }
                sb.append("</sem:triple>\n");

            }

            @Override
            public void quad(Quad quad) {
                String graph = quad.getGraph().getURI();
                if (graph == null) {
                    graph = "default";
                }
                Triple triple = quad.asTriple();

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
                triple(graphCache.get(graph), triple);
            }

            @Override
            public void base(String base) {

            }

            @Override
            public void prefix(String prefix, String iri) {
                logger.debug("I got a prefix");
            }

            @Override
            public void finish() {
                logger.debug("finish called.  map has " + graphCache.size() + " partial docs");
                endDoc();
                for (String key : graphCache.keySet()) {
                    //logger.debug("Cleaning up partial graphs: " + key);
                    endDoc(key);
                }
                batcher.flushAndWait();
            }
        };
    }

    protected String genUri(Path entry) {
        return entry
                .toUri()
                .toString()
                .replaceFirst("^.*\\/jena-benchmark\\/", "/") + ".xml";
    }

    private DatabaseClient client() {
        return DatabaseClientFactory.newClient("localhost",8000,
        //return DatabaseClientFactory.newClient(HOST,8000,
                new DatabaseClientFactory.DigestAuthContext(
                "admin", "admin"));
    }


    public void xcc600kTriples() throws URISyntaxException, XccConfigException, RequestException, InterruptedException, ExecutionException {

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(18);
        URI uri = new URI("xcc://admin:admin@localhost:8000/Documents");
        ContentSource contentSource =
                ContentSourceFactory.newContentSource (uri);




        List<Future<?>> futures = new ArrayList<Future<?>>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get("data/xmltriples"))) {
            for (Path entry : stream) {
                //logger.debug("Adding " + entry.getFileName().toString());

                futures.add(executor.submit( ()  -> {
                    String docUri = genUri(entry);
                    ContentCreateOptions options = ContentCreateOptions.newXmlInstance();

                    String xml = null;
                    try {
                        xml = new String(Files.readAllBytes(entry));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    Content content;
                    content = ContentFactory.newContent(docUri, xml, options);

                    Session session = contentSource.newSession();
                    try {
                        session.insertContent(content);
                    } catch (RequestException e) {
                        e.printStackTrace();
                    }
                    logger.debug("Inserted document " + docUri);
                }
                ));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(Future<?> future : futures) {
            logger.debug("Blocking on thread");
            future.get();
        }

    }
    @Benchmark
    // this uses DMSDK, single thread. So it meets criteria of single transaction.
    public void loadDMSDKXMLTriples() {
        DatabaseClient client = client();

        DataMovementManager movementManager = client.newDataMovementManager();
        WriteBatcher batcher = movementManager
                .newWriteBatcher()
                .withBatchSize(200)
                .withThreadCount(18)
                .onBatchSuccess(new WriteBatchListener() {
                    @Override
                    public void processEvent(WriteBatch batch) {
                        logger.debug("Batch loaded");
                    }
                });

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get("data/xmltriples"))) {
            for (Path entry : stream) {
                logger.debug("Adding " + entry.getFileName().toString());
                String uri = genUri(entry);
                batcher.add(uri, new FileHandle(entry.toFile()).withFormat(Format.XML));
                logger.debug("Inserted document " + uri);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        batcher.flushAndWait();
    }

    public void parseQuadsAndLoad() {
        DatabaseClient client = client();

        DataMovementManager movementManager = client.newDataMovementManager();
        WriteBatcher batcher = movementManager
                .newWriteBatcher()
                .withBatchSize(3000)
                .withThreadCount(24)
                //.withTransactionSize(Integer.MAX_VALUE)
                .onBatchSuccess(new WriteBatchListener() {
                    @Override
                    public void processEvent(WriteBatch batch) {
                        logger.debug("Batch loaded");
                    }
                })
                .onBatchFailure(new WriteFailureListener() {
                                    @Override
                                    public void processFailure(WriteBatch batch, Throwable failure) {
                                        //transaction.rollback();
                                    }
                                }
                );


        movementManager.startJob(batcher);
        RDFDataMgr.parse(sink(batcher), "file:data/nquads/Curriculum_Largest.nq");
        //RDFDataMgr.parse(sink, "file:data/ntriples/ntriples600k.nt");

        batcher.flushAndWait();
        movementManager.stopJob(batcher);

    }

    public void parseTurtleAndLoad() {
        DatabaseClient client = client();

        DataMovementManager movementManager = client.newDataMovementManager();
        WriteBatcher batcher = movementManager
                .newWriteBatcher()
                .withBatchSize(4)
                .withThreadCount(24)
                //.withTransactionSize(Integer.MAX_VALUE)
                .onBatchSuccess(new WriteBatchListener() {
                    @Override
                    public void processEvent(WriteBatch batch) {
                        logger.debug("Batch loaded");
                    }
                })
                .onBatchFailure(new WriteFailureListener() {
                    @Override
                    public void processFailure(WriteBatch batch, Throwable failure) {
                        //transaction.rollback();
                    }
                }
                );


        movementManager.startJob(batcher);
        RDFDataMgr.parse(sink(batcher), "file:data/turtletriples/turtle600k.ttl");
        //RDFDataMgr.parse(sink, "file:data/ntriples/ntriples600k.nt");

        batcher.flushAndWait();
        movementManager.stopJob(batcher);
    }


    public void loadWithBatches() {
        DatabaseClient client = client();

        Transaction t = client.openTransaction();
        XMLDocumentManager documentManager = client.newXMLDocumentManager();


        List<Thread> threads = new ArrayList<Thread>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get("data/xmltriples"))) {
            int i=0;
            DocumentWriteSet writeSet = documentManager.newWriteSet();
            for (Path entry : stream) {
                logger.debug("Adding " + entry.getFileName().toString());
                String uri = genUri(entry);
                writeSet.add(uri, new FileHandle(entry.toFile()));
                logger.debug("Inserted document " + uri);
                if (i > 500) {
                    documentManager.write(writeSet, t);
                    writeSet = documentManager.newWriteSet();
                    i=0;
                }
                i++;
            }
            documentManager.write(writeSet, t);

        } catch (IOException e) {
            e.printStackTrace();
            t.rollback();
        }

        t.commit();
    }



    public void oneBigFile() {
        DatabaseClient client = client();
        XMLDocumentManager documentManager = client.newXMLDocumentManager();
        documentManager.write("/xml600k.xml", new FileHandle(new File("data/onebigfile/xml600k.xml")));
    }

    public void jenaLoad() {
        DatasetGraph dsg = new MarkLogicDatasetGraph(new JenaDatabaseClient(client()));
        RDFDataMgr.read(dsg, "file:data/turtletriples/turtle600k.ttl");
    }


    public void rdf4jLoadTurtle() throws IOException {
        MarkLogicRepository repository = new MarkLogicRepository(client());
        repository.initialize();
        MarkLogicRepositoryConnection conn = repository.getConnection();
        conn.add(new File("data/turtletriples/turtle600k.ttl"), "http://example.org/", RDFFormat.TURTLE );
        conn.close();
    }
    public void rdf4jLoadNtriples() throws IOException {
        MarkLogicRepository repository = new MarkLogicRepository(client());
        repository.initialize();
        MarkLogicRepositoryConnection conn = repository.getConnection();
        conn.add(new File("data/ntriples/ntriples600k.nt"), "http://example.org/", RDFFormat.TURTLE );
        conn.close();
    }

    public void rdf4jLoadParsedTriples() throws IOException {
        MarkLogicRepository repository = new MarkLogicRepository(client());
        repository.initialize();
        MarkLogicRepositoryConnection conn = repository.getConnection();
        RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
        Model model = new LinkedHashModel();
        rdfParser.setRDFHandler(new StatementCollector(model));
        URL url = new URL("file:data/turtletriples/turtle600k.ttl");
        InputStream is = url.openStream();
        rdfParser.parse(is, "http://marklogic.com/bah/");
        conn.add(model);
    }

    public String queryTriples() throws JsonProcessingException {
        DatabaseClient client = client();

        SPARQLQueryManager queryManager = client.newSPARQLQueryManager();

        String countAllTriples = "SELECT (COUNT(?s) as ?ct) where {?s ?p ?o}";
        SPARQLQueryDefinition qdef = queryManager.newQueryDefinition(countAllTriples);

        JacksonHandle handle = queryManager.executeSelect(qdef, new JacksonHandle());
        String subjectCount = handle.get().get("results").get("bindings").get(0).get("ct").get("value").asText();
        return subjectCount;

    }

    /////////////////////////////////////////////////
    public void parseTurtleAndLoadForRDF4J() throws IOException{
        DatabaseClient client = client();

        DataMovementManager movementManager = client.newDataMovementManager();
        WriteBatcher batcher = movementManager
                .newWriteBatcher()
                .withBatchSize(4)
                .withThreadCount(24)
                //.withTransactionSize(Integer.MAX_VALUE)
                .onBatchSuccess(new WriteBatchListener() {
                    @Override
                    public void processEvent(WriteBatch batch) {
                        logger.debug("Batch loaded");
                    }
                })
                .onBatchFailure(new WriteFailureListener() {
                                    @Override
                                    public void processFailure(WriteBatch batch, Throwable failure) {
                                        //transaction.rollback();
                                    }
                                }
                );

        InputStream input = new FileInputStream("data/turtletriples/turtle600k.ttl");

        RDFParser parser = Rio.createParser(RDFFormat.TURTLE, SimpleValueFactory.getInstance());
        parser.setRDFHandler(new RDFHandler() {
            StringBuilder sb;
            int i=0;
            int j=0;
            int T_PER_DOC = 1000;

            void startDoc() {
                sb = new StringBuilder();
                sb.append("<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">\n");
            }
            void endDoc() {
                sb.append("</sem:triples>\n");
                batcher.add("/" + j++ + ".xml", new StringHandle(sb.toString()).withFormat(Format.XML));
                try {
                    Thread.sleep(15);  // avoid starting multiple transactions
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            @Override
            public void startRDF() throws RDFHandlerException {
                startDoc();
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                endDoc();
                batcher.flushAndWait();
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

        movementManager.startJob(batcher);

        batcher.flushAndWait();
        movementManager.stopJob(batcher);
    }
    /////////////////////////////////////////////////

    public static void main(String[] args) throws Exception {
        XMLTripleLoader loader = new XMLTripleLoader();

        Long time = System.currentTimeMillis();


        //loader.parseQuadsAndLoad();
        //loader.parseTurtleAndLoad();
        //loader.xcc600kTriples();
        //loader.loadDMSDKXMLTriples();
        //loader.rdf4jLoadParsedTriples();
        //loader.loadWithBatches();
        //loader.jenaLoad();
        //loader.rdf4jLoadTurtle();
        //loader.rdf4jLoadNtriples();
        //loader.oneBigFile();
        loader.parseTurtleAndLoadForRDF4J();
        System.out.println("Time since load began:" + (System.currentTimeMillis() - time)/1000 + " seconds");
        System.out.println("Triple count: " + loader.queryTriples());
        System.out.println("Time since load began:" + (System.currentTimeMillis() - time)/1000 + " seconds");
        System.exit(0);
    }
}
