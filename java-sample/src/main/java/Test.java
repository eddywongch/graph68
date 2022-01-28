

import java.net.InetSocketAddress;
import java.lang.Integer;
import java.util.Collections;

import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.core.graph.predicates.Search;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.CqlSession;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;


import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;

import com.datastax.dse.driver.api.core.graph.DseGraph;
import static com.datastax.dse.driver.api.core.graph.DseGraph.g;

public class Test {
    public static void main(String[] args) {

        try {
    
            // This is a method to set driver prarameters
            // thru an options map

            OptionsMap options = OptionsMap.driverDefaults();
            // set the datacenter to dc1 in the default profile; this makes dc1 the local datacenter
            //options.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1");
            
            // set the datacenter to dc2 in the "remote" profile
            //options.put("remote", TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc2");
            
            // make sure to provide a contact point belonging to dc1, not dc2!
            //options.put(TypedDriverOption.CONTACT_POINTS, java.util.Collections.singletonList("10.101.33.239:9042"));
            
            // in this example, the default consistency level is LOCAL_QUORUM
            options.put(TypedDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM");
            
            // but when failing over, the consistency level will be automatically downgraded to LOCAL_ONE
            //options.put("remote", TypedDriverOption.REQUEST_CONSISTENCY, "LOCAL_ONE");

            options.put(TypedDriverOption.GRAPH_NAME, "mtb_final");

            // We can set param in the CqlSession object itself
            CqlSession session = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromMap(options))
                .addContactPoint(new InetSocketAddress("10.101.33.239", 9042))
                .withLocalDatacenter("DC1")
                .build();
    

            GraphTraversalSource g =
                AnonymousTraversalSource.traversal().withRemote(DseGraph.remoteConnectionBuilder(session).build());

            GraphTraversal traversal = g.V().has("Company","companyName","DataStax");
            FluentGraphStatement statement2 = FluentGraphStatement.newInstance(traversal);

            GraphResultSet result = session.execute(statement2);
            System.out.println(result.one());

        }
        catch (Exception e){
            System.out.println("Exeption:  " + e);
        }

    }
}
