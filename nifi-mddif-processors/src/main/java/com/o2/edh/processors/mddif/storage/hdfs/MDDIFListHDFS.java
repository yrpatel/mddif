package com.o2.edh.processors.mddif.storage.hdfs;

/**
 * Created by aditya.thakare on 08/05/2019.
 */

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;

import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.AbstractHadoopProcessor;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;


@TriggerSerially
@TriggerWhenEmpty
//@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"hadoop", "HDFS", "get", "list", "ingest", "source", "filesystem"})
@CapabilityDescription("Retrieves a listing of files from HDFS. Each time a listing is performed, the files with the latest timestamp will be excluded "
        + "and picked up during the next execution of the processor. This is done to ensure that we do not miss any files, or produce duplicates, in the "
        + "cases where files with the same timestamp are written immediately before and after a single execution of the processor. For each file that is "
        + "listed in HDFS, this processor creates a FlowFile that represents the HDFS file to be fetched in conjunction with FetchHDFS. This Processor is "
        +  "designed to run on Primary Node only in a cluster. If the primary node changes, the new Primary Node will pick up where the previous node left "
        +  "off without duplicating all of the data. Unlike GetHDFS, this Processor does not delete any data from HDFS.")
@WritesAttributes({
        @WritesAttribute(attribute="filename", description="The name of the file that was read from HDFS."),
        @WritesAttribute(attribute="path", description="The path is set to the absolute path of the file's directory on HDFS. For example, if the Directory property is set to /tmp, "
                + "then files picked up from /tmp will have the path attribute set to \"./\". If the Recurse Subdirectories property is set to true and a file is picked up "
                + "from /tmp/abc/1/2/3, then the path attribute will be set to \"/tmp/abc/1/2/3\"."),
        @WritesAttribute(attribute="hdfs.owner", description="The user that owns the file in HDFS"),
        @WritesAttribute(attribute="hdfs.group", description="The group that owns the file in HDFS"),
        @WritesAttribute(attribute="hdfs.lastModified", description="The timestamp of when the file in HDFS was last modified, as milliseconds since midnight Jan 1, 1970 UTC"),
        @WritesAttribute(attribute="hdfs.length", description="The number of bytes in the file in HDFS"),
        @WritesAttribute(attribute="hdfs.replication", description="The number of HDFS replicas for hte file"),
        @WritesAttribute(attribute="hdfs.permissions", description="The permissions for the file in HDFS. This is formatted as 3 characters for the owner, "
                + "3 for the group, and 3 for other users. For example rw-rw-r--")
})
@Stateful(scopes = Scope.CLUSTER, description = "After performing a listing of HDFS files, the latest timestamp of all the files listed and the latest "
        + "timestamp of all the files transferred are both stored. This allows the Processor to list only files that have been added or modified after "
        + "this date the next time that the Processor is run, without having to store all of the actual filenames/paths which could lead to performance "
        + "problems. State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary "
        + "Node is selected, the new node can pick up where the previous node left off, without duplicating the data.")

public class MDDIFListHDFS extends AbstractHadoopProcessor {

//    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
//        .name("Distributed Cache Service")
//        .description("Specifies the Controller Service that should be used to maintain state about what has been pulled from HDFS so that if a new node "
//                + "begins pulling data, it won't duplicate all of the work that has been done.")
//        .required(false)
//        .identifiesControllerService(DistributedMapCacheClient.class)
//        .build();

    public static final PropertyDescriptor RECURSE_SUBDIRS = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether to list files from subdirectories of the HDFS directory")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue("[^\\.].*")
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed flowFiles are transferred to this relationship")
            .build();

    private volatile long latestTimestampListed = -1L;
    private volatile long latestTimestampEmitted = -1L;
    private volatile long lastRunTimestamp = -1L;

    static final String LISTING_TIMESTAMP_KEY = "listing.timestamp";
    static final String EMITTED_TIMESTAMP_KEY = "emitted.timestamp";

    static final long LISTING_LAG_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);
    }

    protected File getPersistenceFile() {
        return new File("conf/state/" + getIdentifier());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> props = new ArrayList<>(properties);
        //props.add(DISTRIBUTED_CACHE_SERVICE);
        props.add(DIRECTORY);
        props.add(RECURSE_SUBDIRS);
        props.add(FILE_FILTER);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    protected String getKey(final String directory) {
        return getIdentifier() + ".lastListingTime." + directory;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        if (isConfigurationRestored() && (descriptor.equals(DIRECTORY) || descriptor.equals(FILE_FILTER))) {
            latestTimestampEmitted = -1L;
            latestTimestampListed = -1L;
        }
    }

    /**
     * Determines which of the given FileStatus's describes a File that should be listed.
     *
     * @param statuses the eligible FileStatus objects that we could potentially list
     * @return a Set containing only those FileStatus objects that we want to list
     */
    Set<FileStatus> determineListable(final Set<FileStatus> statuses) {
        final long minTimestamp = this.latestTimestampListed;
        final TreeMap<Long, List<FileStatus>> orderedEntries = new TreeMap<>();

        // Build a sorted map to determine the latest possible entries
        for (final FileStatus status : statuses) {
            if (status.getPath().getName().endsWith("_COPYING_")) {
                continue;
            }

            final long entityTimestamp = status.getModificationTime();

            if (entityTimestamp > latestTimestampListed) {
                latestTimestampListed = entityTimestamp;
            }

            // New entries are all those that occur at or after the associated timestamp
            final boolean newEntry = entityTimestamp >= minTimestamp && entityTimestamp > latestTimestampEmitted;

            if (newEntry) {
                List<FileStatus> entitiesForTimestamp = orderedEntries.get(status.getModificationTime());
                if (entitiesForTimestamp == null) {
                    entitiesForTimestamp = new ArrayList<FileStatus>();
                    orderedEntries.put(status.getModificationTime(), entitiesForTimestamp);
                }
                entitiesForTimestamp.add(status);
            }
        }

        final Set<FileStatus> toList = new HashSet<>();
        ArrayList<String> sortedpath = new ArrayList<String>();
        if (orderedEntries.size() > 0) {
            long latestListingTimestamp = orderedEntries.lastKey();

            // If the last listing time is equal to the newest entries previously seen,
            // another iteration has occurred without new files and special handling is needed to avoid starvation
            if (latestListingTimestamp == minTimestamp) {
                // We are done if the latest listing timestamp is equal to the last processed time,
                // meaning we handled those items originally passed over
                if (latestListingTimestamp == latestTimestampEmitted) {
                    return Collections.emptySet();
                }
            } else {
                // Otherwise, newest entries are held back one cycle to avoid issues in writes occurring exactly when the listing is being performed to avoid missing data
                orderedEntries.remove(latestListingTimestamp);
            }

            for (List<FileStatus> timestampEntities : orderedEntries.values()) {
                for (FileStatus status : timestampEntities) {
                    toList.add(status);
                    sortedpath.add((status.getPath()).getName());
                }
            }
        }
        final Set<FileStatus> toSortedList = new HashSet<>();
        final Set<FileStatus> toSortedList1 = new HashSet<>();
        List<FileStatus> list=asSortedList(toList);

        for (int i = 0; i <list.size() ; i++) {
            //System.out.println("Sorted for " + list.get(i).toString());
            toSortedList.add(list.get(i));

        }
        //System.out.println("unsorted "+sortedpath.toString());
        Collections.sort(sortedpath);
        //System.out.println("sorted "+ sortedpath.toString());

        for (int i = 0; i <sortedpath.size() ; i++) {


            // findIfPresent(sortedpath.get(i),toSortedList));
            for (FileStatus fileStatus : toSortedList) {

                if (fileStatus.getPath().getName().equalsIgnoreCase(sortedpath.get(i))){

                    // System.out.println("file found"+ fileStatus.getPath().getName());
                    toSortedList1.add(fileStatus);

                }
            }
        }

        //need to find in set and  insert in sorted list
        return toSortedList1;
    }



    public FileStatus findIfPresent(String source, Set<FileStatus> set){

        for (FileStatus fileStatus : set) {

            if (fileStatus.getPath().getName().equalsIgnoreCase(source)){
                // System.out.println("file found"+ fileStatus.getPath().getName());
                return fileStatus;
            }
        }

        getLogger().error(" source File moved from directory");


        return null;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        //Do Nothing
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // We have to ensure that we don't continually perform listings, because if we perform two listings within
        // the same millisecond, our algorithm for comparing timestamps will not work. So we ensure here that we do
        // not let that happen.

        FlowFile inputFlowFile = session.get();
        if ( inputFlowFile == null ) {
            return;
        }



//        final long now = System.nanoTime();
//        if (now - lastRunTimestamp < LISTING_LAG_NANOS) {
//            lastRunTimestamp = now;
//            context.yield();
//            return;
//        }
//        lastRunTimestamp = now;

        getLogger().info("Received Path: {}", new Object[] {context.getProperty(DIRECTORY).evaluateAttributeExpressions(inputFlowFile).getValue()});
        getLogger().info("Received File Filter: {}", new Object[] {context.getProperty(FILE_FILTER).evaluateAttributeExpressions(inputFlowFile).getValue()});

        final String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions(inputFlowFile).getValue();

        // Ensure that we are using the latest listing information before we try to perform a listing of HDFS files.
        try {
            final StateMap stateMap = context.getStateManager().getState(Scope.CLUSTER);
            if (stateMap.getVersion() == -1L) {
                latestTimestampEmitted = -1L;
                latestTimestampListed = -1L;
                getLogger().debug("Found no state stored");
            } else {
                // Determine if state is stored in the 'new' format or the 'old' format
                final String emittedString = stateMap.get(EMITTED_TIMESTAMP_KEY);
                if (emittedString == null) {
                    latestTimestampEmitted = -1L;
                    latestTimestampListed = -1L;
                    getLogger().debug("Found no recognized state keys; assuming no relevant state and resetting listing/emitted time to -1");
                } else {
                    // state is stored in the new format, using just two timestamps
                    latestTimestampEmitted = Long.parseLong(emittedString);
                    final String listingTimestmapString = stateMap.get(LISTING_TIMESTAMP_KEY);
                    if (listingTimestmapString != null) {
                        latestTimestampListed = Long.parseLong(listingTimestmapString);
                    }

                    getLogger().debug("Found new-style state stored, latesting timestamp emitted = {}, latest listed = {}",
                            new Object[] {latestTimestampEmitted, latestTimestampListed});
                }
            }
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve timestamp of last listing from Distributed Cache Service. Will not perform listing until this is accomplished.");
            session.transfer(inputFlowFile,REL_FAILURE);
            context.yield();
            return;
        }

        // Pull in any file that is newer than the timestamp that we have.
        final FileSystem hdfs = getFileSystem();
        final boolean recursive = context.getProperty(RECURSE_SUBDIRS).asBoolean();

        final Set<FileStatus> statuses;
        try {
            final Path rootPath = new Path(directory);
            statuses = getStatuses(rootPath, recursive, hdfs, createPathFilter(context, inputFlowFile));
            getLogger().debug("Found a total of {} files in HDFS", new Object[] {statuses.size()});
        } catch (final IOException | IllegalArgumentException e) {
            getLogger().error("Failed to perform listing of HDFS due to {}", new Object[] {e});
            session.transfer(inputFlowFile,REL_FAILURE);
            context.yield();
            return;
        }

        //final Set<FileStatus> listable = determineListable(statuses);
        //getLogger().debug("Of the {} files found in HDFS, {} are listable", new Object[] {statuses.size(), listable.size()});


        List<FileStatus> list=asSortedList(statuses); // not using this "listable", as we need to list all the files.




        //listable=new TreeSet<>(listable);

        //System.out.println(listable.toString());
        final AtomicReference<String> value = new AtomicReference<>();
        session.read(inputFlowFile, inputStream -> {
            try {
                value.set(IOUtils.toString(inputStream, "UTF-8"));
            } catch (Exception e) {
                getLogger().error("Caught Exception: "+ e.getMessage());
                e.printStackTrace();
                session.transfer(inputFlowFile,REL_FAILURE);
                context.yield();
                return;
            }
        });

        String jsonString = value.get();
        JsonObject jsonObject = (JsonObject) new JsonParser().parse(jsonString);
        String registry_id = jsonObject.get("id").getAsString();
        //String file_path = jsonObject.get("remote_path").getAsString();


        String ipAddress = "";
        try {
            ipAddress = InetAddress.getLocalHost().getHostAddress();
            ipAddress = ipAddress.substring(ipAddress.lastIndexOf('.')+1);
        } catch (UnknownHostException e) {
            getLogger().error("Caught Exception: "+ e.getMessage());
            e.printStackTrace();
            session.transfer(inputFlowFile,REL_FAILURE);
            context.yield();
            return;
        }

        for (final FileStatus status : list) {

            final Map<String, String> attributes = createAttributes(status);
            FlowFile flowFile = session.create(inputFlowFile);
            flowFile = session.putAllAttributes(flowFile, attributes);

            String timeString = getTime();

            JsonArray ja = new JsonArray();
            JsonObject jo = new JsonObject();
            jo.addProperty("run_dt", timeString);
            jo.addProperty("run_uuid", ipAddress+"-"+UUID.randomUUID().toString());
            jo.addProperty("registry_id", registry_id);
            jo.addProperty("file_name", status.getPath().getName());
            jo.addProperty("file_path", getAbsolutePath(status.getPath().getParent()));
            jo.addProperty("file_size", String.valueOf(status.getLen()));
            jo.addProperty("is_fetched", "0"); //bit(1)
            jo.addProperty("is_locked", "0");  //bit(1)
            jo.addProperty("is_success", "0");  //bit(1)
            ja.add(jo);

            flowFile = session.putAttribute(flowFile, "run_dt", timeString);
            flowFile = session.write(flowFile, outputStream -> outputStream.write(ja.toString().getBytes()));

            session.transfer(flowFile, REL_SUCCESS);
            //System.out.println(flowFile.getAttribute("filename"));

            final long fileModTime = status.getModificationTime();
            if (fileModTime > latestTimestampEmitted) {
                latestTimestampEmitted = fileModTime;
            }
        }

        session.remove(inputFlowFile);
        final int listCount = list.size();
        if ( listCount > 0 ) {
            getLogger().info("Successfully created listing with {} new files from HDFS", new Object[] {listCount});
            session.commit();
        } else {
            getLogger().debug("There is no data to list.");
            // context.yield();
        }

        final Map<String, String> updatedState = new HashMap<>(1);
        updatedState.put(LISTING_TIMESTAMP_KEY, String.valueOf(latestTimestampListed));
        updatedState.put(EMITTED_TIMESTAMP_KEY, String.valueOf(latestTimestampEmitted));
        getLogger().debug("New state map: {}", new Object[] {updatedState});

        try {
            context.getStateManager().setState(updatedState, Scope.CLUSTER);
        } catch (final IOException ioe) {
            getLogger().warn("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", ioe);
        }


    }
    public String getTime() {
        return new SimpleDateFormat("YYYY-MM-dd HH:mm:ss.SSS").format(Calendar.getInstance().getTime()); // AT: removed 'Z' to support MySQL format.
    }

    public static    <T extends Comparable<? super T>> List<T> asSortedList(Collection<T> c) {
        List<T> list = new ArrayList<T>(c);
        Collections.sort(list);
        return list;
    }
    private Set<FileStatus> getStatuses(final Path path, final boolean recursive, final FileSystem hdfs, final PathFilter filter) throws IOException {
        final Set<FileStatus> statusSet = new HashSet<>();

        getLogger().debug("Fetching listing for {}", new Object[] {path});
        final FileStatus[] statuses = hdfs.listStatus(path, filter);

        for ( final FileStatus status : statuses ) {
            if (status.isDirectory()) {
                if ( recursive ) {
                    try {
                        statusSet.addAll(getStatuses(status.getPath(), recursive, hdfs, filter));
                    } catch (final IOException ioe) {
                        getLogger().error("Failed to retrieve HDFS listing for subdirectory {} due to {}; will continue listing others", new Object[] {status.getPath(), ioe});
                    }
                }
            } else {
                statusSet.add(status);
            }
        }

        return statusSet;
    }

    private String getAbsolutePath(final Path path) {
        final Path parent = path.getParent();
        final String prefix = (parent == null || parent.getName().equals("")) ? "" : getAbsolutePath(parent);
        return prefix + "/" + path.getName();
    }

    private Map<String, String> createAttributes(final FileStatus status) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), status.getPath().getName());
        attributes.put(CoreAttributes.PATH.key(), getAbsolutePath(status.getPath().getParent()));

        attributes.put("hdfs.owner", status.getOwner());
        attributes.put("hdfs.group", status.getGroup());
        attributes.put("hdfs.lastModified", String.valueOf(status.getModificationTime()));
        attributes.put("hdfs.length", String.valueOf(status.getLen()));
        attributes.put("hdfs.replication", String.valueOf(status.getReplication()));

        final FsPermission permission = status.getPermission();
        final String perms = getPerms(permission.getUserAction()) + getPerms(permission.getGroupAction()) + getPerms(permission.getOtherAction());
        attributes.put("hdfs.permissions", perms);
        return attributes;
    }

    private String getPerms(final FsAction action) {
        final StringBuilder sb = new StringBuilder();
        if (action.implies(FsAction.READ)) {
            sb.append("r");
        } else {
            sb.append("-");
        }

        if (action.implies(FsAction.WRITE)) {
            sb.append("w");
        } else {
            sb.append("-");
        }

        if (action.implies(FsAction.EXECUTE)) {
            sb.append("x");
        } else {
            sb.append("-");
        }

        return sb.toString();
    }

    private PathFilter createPathFilter(final ProcessContext context , FlowFile inFile ) {
        final Pattern filePattern = Pattern.compile(context.getProperty(FILE_FILTER).evaluateAttributeExpressions(inFile).getValue());
        return new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return filePattern.matcher(path.getName()).matches();
            }
        };
    }
}
