package com.madebykawet.couchbaseliteandroidtests;

import android.os.AsyncTask;
import android.os.Bundle;
import android.support.v7.app.ActionBarActivity;
import android.util.Log;
import android.view.View;
import android.widget.TextView;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.Document;
import com.couchbase.lite.Emitter;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Mapper;
import com.couchbase.lite.TransactionalTask;
import com.couchbase.lite.android.AndroidContext;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class MainActivity extends ActionBarActivity {
    private TextView textView;
    private TextView smallDocValueSizeView;
    private TextView smallDocCountView;
    private TextView bigDocValueSizeView;
    private TextView bigDocCountView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        textView = (TextView) findViewById(R.id.textView);
        smallDocValueSizeView = (TextView) findViewById(R.id.smallDocSize);
        smallDocCountView = (TextView) findViewById(R.id.smallDocCount);
        bigDocValueSizeView = (TextView) findViewById(R.id.bigDocSize);
        bigDocCountView = (TextView) findViewById(R.id.bigDocCount);
    }

    public void onClick_Init(View v) {
        initDb();
    }

    public void onClick_Index(View v) {
        index(1);
    }

    public void onClick_IndexBatch(View v) {
        index(2);
    }

    public void onClick_IndexOptimized(View v) {
        index(3);
    }

    public void initDb() {
        textView.setText("Creating db with data");
        final int smallDocSize = Integer.parseInt(smallDocValueSizeView.getText().toString());
        final int smallDocCount = Integer.parseInt(smallDocCountView.getText().toString());
        final int bigDocSize = Integer.parseInt(bigDocValueSizeView.getText().toString());
        final int bigDocCount = Integer.parseInt(bigDocCountView.getText().toString());
        new AsyncTask<Void, Void, Void>() {
            @Override
            protected Void doInBackground(Void[] params) {
                try {
                    String dbName = "testdb";
                    Manager.enableLogging(com.couchbase.lite.util.Log.TAG_VIEW, com.couchbase.lite.util.Log.VERBOSE);
                    Manager manager = new Manager(new AndroidContext(MainActivity.this), Manager.DEFAULT_OPTIONS);
                    manager.getDatabase(dbName).delete();
                    final Database database = manager.getDatabase(dbName);
                    // Load a json documents
                    StringBuffer smallValue = new StringBuffer(smallDocSize);
                    for (int i = 0; i < smallDocSize; i++)
                        smallValue.append("a");
                    String smallJson = "{\"small_field\":\"" + smallValue.toString() + "\"}";
                    StringBuffer bigValue = new StringBuffer(bigDocSize);
                    for (int i = 0; i < bigDocSize; i++)
                        bigValue.append("a");
                    String bigJson = "{\"big_field\":\"" + bigValue.toString() + "\"}";
                    ObjectMapper mapper = new ObjectMapper();
                    final Map<String, Object> smallProperties = mapper.readValue(smallJson, new TypeReference<HashMap<String, Object>>() {
                    });
                    final Map<String, Object> bigProperties = mapper.readValue(bigJson, new TypeReference<HashMap<String, Object>>() {
                    });
                    // Add documents in database
                    database.runInTransaction(new TransactionalTask() {
                        @Override
                        public boolean run() {
                            try {
                                for (int i = 0; i < smallDocCount; i++) {
                                    Document doc = database.createDocument();
                                    doc.putProperties(smallProperties);
                                }
                                for (int i = 0; i < bigDocCount; i++) {
                                    Document doc = database.createDocument();
                                    doc.putProperties(bigProperties);
                                }
                            } catch (CouchbaseLiteException e) {
                                e.printStackTrace();
                            }
                            return true;
                        }
                    });
                    database.close();
                } catch (CouchbaseLiteException | IOException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            protected void onPostExecute(Void nothing) {
                TextView text = (TextView) findViewById(R.id.textView);
                text.setText("Db created and data inserted");
            }
        }.execute();
    }

    private void index(final int type) {
        textView.setText("Updating view");
        new AsyncTask<Void, Void, Void>() {
            private long ms;
            private int count;

            @Override
            protected Void doInBackground(Void[] params) {
                try {
                    String dbName = "testdb";
                    Manager.enableLogging(com.couchbase.lite.util.Log.TAG_VIEW, com.couchbase.lite.util.Log.VERBOSE);
                    Manager manager = new Manager(new AndroidContext(MainActivity.this), Manager.DEFAULT_OPTIONS);
                    final Database database = manager.getDatabase(dbName);
                    // Add a view
                    com.couchbase.lite.View view = database.getView("TestView");
                    view.setMap(new Mapper() {
                        @Override
                        public void map(Map<String, Object> props, Emitter emiter) {
                            if (props.containsKey("big_field"))
                                emiter.emit(props.get("_id"), null);
                        }
                    }, "1.0");
                    // Update view index
                    view.deleteIndex();
                    long start = System.currentTimeMillis();
                    Log.d("UpdateView", "START INDEXING VIEW");
                    if (type == 1)
                        view.updateIndex();
                    else if(type == 2)
                        view.updateIndex(200);
                    else
                        view.updateIndexOptimized();
                    long end = System.currentTimeMillis();
                    Log.d("UpdateView", "STOP INDEXING VIEW");
                    Log.d("UpdateView", (end - start) + "ms");
                    Log.d("UpdateView", "View contains " + view.getTotalRows() + " rows");
                    this.ms = end - start;
                    this.count = view.getTotalRows();
                    database.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (CouchbaseLiteException e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            protected void onPostExecute(Void nothing) {
                TextView text = (TextView) findViewById(R.id.textView);
                text.setText("View updated: " + this.ms + "ms / indexed " + this.count + " rows");
            }
        }.execute();
    }
}