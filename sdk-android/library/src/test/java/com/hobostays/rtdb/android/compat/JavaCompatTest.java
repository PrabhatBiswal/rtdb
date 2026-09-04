package com.hobostays.rtdb.android.compat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.hobostays.rtdb.android.FakeWire;
import com.hobostays.rtdb.android.MainThreadExecutor;
import com.hobostays.rtdb.android.TestClients;
import com.hobostays.rtdb.api.RtdbClient;
import com.hobostays.rtdb.api.RtdbError;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.annotation.Config;

/**
 * The wrapper's real contract, written in JAVA because that is what the 76 files are written in.
 * A Kotlin test would prove the logic but not the ergonomics — no named arguments, no default
 * parameters, no extension functions, and every listener an anonymous class, exactly as the app
 * writes them today.
 *
 * It drives a real RtdbClient over a fake socket, so what is asserted is the whole path: Java call
 * -> JsonElement -> wire, and wire -> mirror -> JsonElement -> Java types.
 */
@RunWith(RobolectricTestRunner.class)
@Config(sdk = 23)
public class JavaCompatTest {

    private FakeWire wire;
    private RtdbClient client;
    private RtdbDatabase database;

    /** Everything a listener was told, as readable strings, in order. */
    private final List<String> events = new CopyOnWriteArrayList<>();

    @Before
    public void connect() {
        wire = new FakeWire();
        client = TestClients.connected(wire);
        database = new RtdbDatabase(client);
    }

    @After
    public void close() {
        client.close();
    }

    // ------------------------------------------------------------------ the namespace helper

    @Test
    public void referenceCollapsesADatabaseNameAndTableIntoOnePath() {
        DatabaseReference ref = database.reference("liveChatDb", "MPK_1010");
        assertEquals("liveChatDb/MPK_1010", ref.getPath());
        assertEquals("MPK_1010", ref.getKey());
        assertEquals("liveChatDb/MPK_1010/1474396", ref.child("1474396").getPath());
    }

    // ------------------------------------------------------------------ writes (§4)

    @Test
    public void everyShapeTheAuditFoundConvertsAtTheBoundary() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "Ravi");
        map.put("score", 42);
        map.put("online", true);
        map.put("tag", null);

        database.getReference("room").setValue(map);
        database.getReference("room/name").setValue("Ravi");
        database.getReference("room/score").setValue(42);
        database.getReference("room/online").setValue(true);
        database.getReference("room/gone").setValue(null);
        database.getReference("room/list").setValue(new ArrayList<>(List.of(1, 2, 3)));
        database.getReference("room/removed").removeValue();

        // Writes are posted to the client's dispatcher, so the wire catches up a moment later.
        wire.awaitSent("put", 7);
        List<String> puts = wire.sentValues("put");
        assertEquals("{\"name\":\"Ravi\",\"score\":42,\"online\":true,\"tag\":null}", puts.get(0));
        assertEquals("\"Ravi\"", puts.get(1));
        assertEquals("42", puts.get(2));
        assertEquals("true", puts.get(3));
        assertEquals("null", puts.get(4));
        assertEquals("[1,2,3]", puts.get(5));
        assertEquals("removeValue() is a put of null (§4)", "null", puts.get(6));
    }

    @Test
    public void updateChildrenSendsOneMergeForTheWholeMap() {
        Map<String, Object> hashMap = new LinkedHashMap<>();
        hashMap.put("score", 50);
        hashMap.put("online", false);

        database.reference("liveChatDb", "MPK_1010").child("1474396").updateChildren(hashMap);

        wire.awaitSent("merge", 1);
        assertEquals(1, wire.sentValues("merge").size());
        assertEquals("{\"score\":50,\"online\":false}", wire.sentValues("merge").get(0));
        assertEquals("liveChatDb/MPK_1010/1474396", wire.sentPaths("merge").get(0));
    }

    @Test
    public void aClassTheSdkCannotExpressFailsAtTheCallSite() {
        try {
            database.getReference("room").setValue(new Object());
            fail("expected the boundary to refuse a class with no serializer");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("no reflection mapping"));
        }
    }

    // ------------------------------------------------------------------ reads (§7)

    @Test
    public void valueListenersSeePlainJavaTypesAndTypedModels() {
        database.getReference("UserStatus/u1").addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot snapshot) {
                events.add("value:" + snapshot.getValue());
                events.add("name:" + snapshot.child("name").getValue(String.class));
                events.add("score:" + snapshot.child("score").getValue(Long.class));
                events.add("online:" + snapshot.child("online").getValue(Boolean.class));
                events.add("missing:" + snapshot.child("nope").getValue(String.class));
                events.add("exists:" + snapshot.exists() + "/" + snapshot.child("nope").exists());
                events.add("hasChild:" + snapshot.hasChild("name"));
                events.add("key:" + snapshot.getKey());

                UserStatus model = snapshot.getValue(UserStatus.class);
                events.add("model:" + model.getName() + "/" + model.getOnline() + "/" + model.getScore());

                List<String> children = new ArrayList<>();
                for (DataSnapshot child : snapshot.getChildren()) {
                    children.add(child.getKey());
                }
                events.add("children:" + children);
            }

            @Override
            public void onCancelled(RtdbError error) {
                events.add("cancelled:" + error.getCode());
            }
        });

        wire.awaitSent("listen", 1);
        wire.deliver("{\"type\":\"snapshot\",\"subId\":1,\"path\":\"UserStatus/u1\","
                + "\"value\":{\"name\":\"Ravi\",\"score\":42,\"online\":true},\"rev\":7}");
        await(10);

        assertEquals(
                List.of(
                        "value:{name=Ravi, score=42, online=true}",
                        "name:Ravi",
                        "score:42",
                        "online:true",
                        "missing:null",
                        "exists:true/false",
                        "hasChild:true",
                        "key:u1",
                        "model:Ravi/true/42",
                        "children:[name, score, online]"),
                events);
    }

    @Test
    public void aModelMissingAFieldFallsBackToItsDefault() {
        database.getReference("UserStatus/u1").addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot snapshot) {
                UserStatus model = snapshot.getValue(UserStatus.class);
                events.add(model.getName() + "/" + model.getOnline() + "/" + model.getScore());
            }

            @Override
            public void onCancelled(RtdbError error) {}
        });

        wire.awaitSent("listen", 1);
        // No `online`, and an `extra` the model has never heard of — both survivable, because a
        // Firebase model is always older or newer than the data somewhere in a rollout.
        wire.deliver("{\"type\":\"snapshot\",\"subId\":1,\"path\":\"UserStatus/u1\","
                + "\"value\":{\"name\":\"Ravi\",\"score\":42,\"extra\":\"ignored\"},\"rev\":7}");
        await(1);

        assertEquals(List.of("Ravi/false/42"), events);
    }

    @Test
    public void childListenersGetTheThreeMethodsAndNothingElse() {
        database.getReference("room").addChildEventListener(new ChildEventListener() {
            @Override
            public void onChildAdded(DataSnapshot snapshot) {
                events.add("added:" + snapshot.getKey() + "=" + snapshot.getValue());
            }

            @Override
            public void onChildChanged(DataSnapshot snapshot) {
                events.add("changed:" + snapshot.getKey() + "=" + snapshot.getValue());
            }

            @Override
            public void onChildRemoved(DataSnapshot snapshot) {
                events.add("removed:" + snapshot.getKey() + "=" + snapshot.getValue());
            }

            @Override
            public void onCancelled(RtdbError error) {
                events.add("cancelled:" + error.getCode());
            }
        });

        wire.awaitSent("listen", 1);
        wire.deliver("{\"type\":\"snapshot\",\"subId\":1,\"path\":\"room\",\"value\":{\"a\":1},\"rev\":7}");
        await(1);
        wire.deliver("{\"type\":\"delta\",\"rev\":8,\"path\":\"room/b\",\"op\":\"put\",\"value\":2}");
        await(2);
        wire.deliver("{\"type\":\"delta\",\"rev\":9,\"path\":\"room/a\",\"op\":\"put\",\"value\":9}");
        await(3);
        wire.deliver("{\"type\":\"delta\",\"rev\":10,\"path\":\"room/b\",\"op\":\"put\",\"value\":null}");
        await(4);

        assertEquals(List.of("added:a=1", "added:b=2", "changed:a=9", "removed:b=2"), events);
    }

    @Test
    public void singleValueListenersFireOnceAndUnlisten() {
        database.getReference("UserStatus/u1").addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot snapshot) {
                events.add("once:" + snapshot.getValue());
            }

            @Override
            public void onCancelled(RtdbError error) {}
        });

        wire.awaitSent("listen", 1);
        wire.deliver("{\"type\":\"snapshot\",\"subId\":1,\"path\":\"UserStatus/u1\",\"value\":1,\"rev\":7}");
        await(1);
        wire.awaitSent("unlisten", 1);

        // A second change on the same path must not reach a listener that already fired.
        wire.deliver("{\"type\":\"delta\",\"rev\":8,\"path\":\"UserStatus/u1\",\"op\":\"put\",\"value\":2}");
        FakeWire.drainMainThread();
        assertEquals(List.of("once:1"), events);
    }

    @Test
    public void removeEventListenerWorksFromADifferentReferenceObject() {
        ValueEventListener listener = new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot snapshot) {
                events.add("value:" + snapshot.getValue());
            }

            @Override
            public void onCancelled(RtdbError error) {}
        };
        database.getReference("room").addValueEventListener(listener);
        wire.awaitSent("listen", 1);
        wire.deliver("{\"type\":\"snapshot\",\"subId\":1,\"path\":\"room\",\"value\":1,\"rev\":7}");
        await(1);

        // The app never keeps the reference it registered with — 223 removeEventListener sites, and
        // getReference() hands back a new object every time.
        database.getReference("room").removeEventListener(listener);
        wire.awaitSent("unlisten", 1);

        wire.deliver("{\"type\":\"delta\",\"rev\":8,\"path\":\"room\",\"op\":\"put\",\"value\":2}");
        FakeWire.drainMainThread();
        assertEquals(List.of("value:1"), events);
    }

    @Test
    public void aSubScopedErrorCancelsTheListener() {
        database.getReference("room").addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot snapshot) {
                events.add("value:" + snapshot.getValue());
            }

            @Override
            public void onCancelled(RtdbError error) {
                events.add("cancelled:" + error.getCode() + "/" + error.getMessage());
            }
        });
        wire.awaitSent("listen", 1);
        wire.deliver("{\"type\":\"err\",\"subId\":1,\"code\":\"RULES\",\"msg\":\"read denied\"}");
        await(1);

        assertEquals(List.of("cancelled:RULES/read denied"), events);
    }

    @Test
    public void infoConnectedIsReadableLikeAnyOtherPath() {
        database.getReference(".info/connected").addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot snapshot) {
                events.add("connected:" + snapshot.getValue());
            }

            @Override
            public void onCancelled(RtdbError error) {}
        });
        await(1);
        assertEquals(List.of("connected:true"), events);
        assertFalse(
                "a virtual path is served from the client, never listened for",
                wire.sentTypes().contains("listen"));
    }

    /** The callbacks arrive on the main looper (that is the SDK's contract), so drain it. */
    private void await(int count) {
        FakeWire.awaitMainThread(count + " events", () -> events.size() >= count);
    }
}
