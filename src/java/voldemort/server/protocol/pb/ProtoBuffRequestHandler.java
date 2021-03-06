package voldemort.server.protocol.pb;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VProto;
import voldemort.client.protocol.pb.VProto.GetRequest;
import voldemort.client.protocol.pb.VProto.KeyStatus;
import voldemort.client.protocol.pb.VProto.RequestType;
import voldemort.client.protocol.pb.VProto.UnlockRequest;
import voldemort.client.protocol.pb.VProto.VoldemortRequest;
import voldemort.server.RequestRoutingType;
import voldemort.server.StoreRepository;
import voldemort.server.protocol.AbstractRequestHandler;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.Store;
import voldemort.undoTracker.DBProxy;
import voldemort.undoTracker.DBProxyFactory;
import voldemort.undoTracker.SRD;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * A Protocol Buffers request handler
 * 
 * 
 */
public class ProtoBuffRequestHandler extends AbstractRequestHandler {

    public static final DBProxy undoStub = DBProxyFactory.build();

    public ProtoBuffRequestHandler(ErrorCodeMapper errorMapper, StoreRepository storeRepository) {
        super(errorMapper, storeRepository);
    }

    @Override
    public StreamRequestHandler handleRequest(DataInputStream inputStream,
                                              DataOutputStream outputStream) throws IOException {
        VoldemortRequest.Builder request = ProtoUtils.readToBuilder(inputStream,
                                                                    VoldemortRequest.newBuilder());
        boolean shouldRoute = request.getShouldRoute();
        RequestRoutingType type = RequestRoutingType.getRequestRoutingType(shouldRoute, false);

        if(request.hasRequestRouteType()) {
            type = RequestRoutingType.getRequestRoutingType(request.getRequestRouteType());
        }

        String storeName = request.getStore();
        Store<ByteArray, byte[], byte[]> store = getStore(storeName, type);
        Message response;
        if(store == null) {
            response = unknownStore(storeName, request.getType());
        } else {
            switch(request.getType()) {
                case GET:
                    response = handleGet(request.getGet(), store);
                    break;
                case GET_ALL:
                    response = handleGetAll(request.getGetAll(), store);
                    break;
                case PUT:
                    response = handlePut(request.getPut(), store);
                    break;
                case DELETE:
                    response = handleDelete(request.getDelete(), store);
                    break;
                case GET_VERSION:
                    response = handleGetVersion(request.getGet(), store);
                    break;
                case UNLOCK:
                    response = handleUnlock(request.getUnlock(), store);
                    break;
                default:
                    throw new VoldemortException("Unknown operation " + request.getType());
            }
        }
        ProtoUtils.writeMessage(outputStream, response);
        return null;
    }

    private Message handleUnlock(UnlockRequest request, Store<ByteArray, byte[], byte[]> store) {
        VProto.UnlockResponse.Builder response = VProto.UnlockResponse.newBuilder();
        List<ByteArray> keys = new ArrayList<ByteArray>(request.getKeyCount());
        for(ByteString bs: request.getKeyList()) {
            ByteArray key = ProtoUtils.decodeBytes(bs);
            keys.add(key);
        }

        SRD srd = new SRD(request.getSrd());

        try {
            // TODO deveria ter em conta a store
            Map<ByteArray, Boolean> status = undoStub.unlockKeys(keys, srd);
            for(Entry<ByteArray, Boolean> s: status.entrySet()) {
                KeyStatus.Builder b = KeyStatus.newBuilder()
                                               .setKey(ProtoUtils.encodeBytes(s.getKey()))
                                               .setStatus(s.getValue());
                response.addStatus(b);
            }
        } catch(VoldemortException e) {
            e.printStackTrace();
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        return response.build();
    }

    private Message handleGetVersion(GetRequest request, Store<ByteArray, byte[], byte[]> store) {
        VProto.GetVersionResponse.Builder response = VProto.GetVersionResponse.newBuilder();
        ByteArray key = ProtoUtils.decodeBytes(request.getKey());
        SRD srd = new SRD(request.getSrd());
        undoStub.startOperation(OpType.GetVersion, key, srd);
        try {
            List<Version> versions = store.getVersions(key, srd);
            for(Version version: versions)
                response.addVersions(ProtoUtils.encodeClock(version));
        } catch(VoldemortException e) {
            System.err.println(e);
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        undoStub.endOperation(OpType.GetVersion, key, srd);
        return response.build();
    }

    @Override
    public boolean isCompleteRequest(ByteBuffer buffer) {
        if(buffer.remaining() < 4)
            return false;

        int size = buffer.getInt();
        return buffer.remaining() == size;
    }

    private VProto.GetResponse handleGet(VProto.GetRequest request,
                                         Store<ByteArray, byte[], byte[]> store) {
        VProto.GetResponse.Builder response = VProto.GetResponse.newBuilder();
        ByteArray key = ProtoUtils.decodeBytes(request.getKey());
        SRD srd = new SRD(request.getSrd());
        undoStub.startOperation(OpType.Get, key, srd);

        try {
            List<Versioned<byte[]>> values = store.get(key,
                                                       request.hasTransforms() ? ProtoUtils.decodeBytes(request.getTransforms())
                                                                                           .get()
                                                                              : null,
                                                       srd);
            for(Versioned<byte[]> versioned: values)
                response.addVersioned(ProtoUtils.encodeVersioned(versioned));
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        undoStub.endOperation(OpType.Get, key, srd);
        return response.build();
    }

    private VProto.GetAllResponse handleGetAll(VProto.GetAllRequest request,
                                               Store<ByteArray, byte[], byte[]> store) {

        VProto.GetAllResponse.Builder response = VProto.GetAllResponse.newBuilder();
        // TODO
        try {
            List<ByteArray> keys = new ArrayList<ByteArray>(request.getKeysCount());
            for(ByteString string: request.getKeysList())
                keys.add(ProtoUtils.decodeBytes(string));

            int transformsSize = request.getTransformsCount();
            Map<ByteArray, byte[]> transforms = null;
            if(transformsSize > 0) {
                for(VProto.GetAllRequest.GetAllTransform transform: request.getTransformsList()) {
                    transforms = new HashMap<ByteArray, byte[]>(transformsSize);
                    transforms.put(ProtoUtils.decodeBytes(transform.getKey()),
                                   ProtoUtils.decodeBytes(transform.getTransform()).get());
                }
            }
            SRD srd = new SRD(request.getSrd());

            Map<ByteArray, List<Versioned<byte[]>>> values = store.getAll(keys, transforms, srd);
            for(Map.Entry<ByteArray, List<Versioned<byte[]>>> entry: values.entrySet()) {
                VProto.KeyedVersions.Builder keyedVersion = VProto.KeyedVersions.newBuilder()
                                                                                .setKey(ProtoUtils.encodeBytes(entry.getKey()));
                for(Versioned<byte[]> version: entry.getValue())
                    keyedVersion.addVersions(ProtoUtils.encodeVersioned(version));
                response.addValues(keyedVersion);
            }
        } catch(VoldemortException e) {
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        return response.build();

    }

    private VProto.PutResponse handlePut(VProto.PutRequest request,
                                         Store<ByteArray, byte[], byte[]> store) {
        VProto.PutResponse.Builder response = VProto.PutResponse.newBuilder();
        ByteArray key = ProtoUtils.decodeBytes(request.getKey());
        SRD srd = new SRD(request.getSrd());
        undoStub.startOperation(OpType.Put, key, srd);

        try {
            Versioned<byte[]> value = ProtoUtils.decodeVersioned(request.getVersioned());

            store.put(key,
                      value,
                      request.hasTransforms() ? ProtoUtils.decodeBytes(request.getTransforms())
                                                          .get() : null,
                      srd);
        } catch(VoldemortException e) {
            e.printStackTrace();
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }
        undoStub.endOperation(OpType.Put, key, srd);
        return response.build();
    }

    private VProto.DeleteResponse handleDelete(VProto.DeleteRequest request,
                                               Store<ByteArray, byte[], byte[]> store) {
        VProto.DeleteResponse.Builder response = VProto.DeleteResponse.newBuilder();
        ByteArray key = ProtoUtils.decodeBytes(request.getKey());
        SRD srd = new SRD(request.getSrd());
        undoStub.startOperation(OpType.Delete, key, srd);

        try {
            boolean success = store.delete(key, ProtoUtils.decodeClock(request.getVersion()), srd);
            response.setSuccess(success);
        } catch(VoldemortException e) {
            response.setSuccess(false);
            response.setError(ProtoUtils.encodeError(getErrorMapper(), e));
        }

        undoStub.endOperation(OpType.Delete, key, srd);
        return response.build();
    }

    public Message unknownStore(String storeName, RequestType type) {
        VProto.Error error = VProto.Error.newBuilder()
                                         .setErrorCode(getErrorMapper().getCode(VoldemortException.class))
                                         .setErrorMessage("Unknown store '" + storeName + "'.")
                                         .build();
        switch(type) {
            case GET:
                return VProto.GetResponse.newBuilder().setError(error).build();
            case GET_ALL:
                return VProto.GetAllResponse.newBuilder().setError(error).build();
            case PUT:
                return VProto.PutResponse.newBuilder().setError(error).build();
            case DELETE:
                return VProto.DeleteResponse.newBuilder().setError(error).setSuccess(false).build();
            default:
                throw new VoldemortException("Unknown operation " + type);
        }
    }

}
