package pdc;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

/**
 * The Master acts as the RPC Coordinator in a distributed cluster.
 * It handles remote procedure call request/response patterns over sockets.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    
    // Worker registry and state
    private final Map<String, WorkerState> workers = new ConcurrentHashMap<>();
    private final BlockingQueue<String> availableWorkers = new LinkedBlockingQueue<>();
    
    // Task management
    private final Map<Integer, ComputeTask> allTasks = new ConcurrentHashMap<>();
    private final BlockingQueue<ComputeTask> pendingTasks = new LinkedBlockingQueue<>();
    private final Set<Integer> completedTasks = ConcurrentHashMap.newKeySet();
    
    // Result storage
    private volatile int[][] resultMatrix;
    
    // Server state
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    
    // Configuration from environment variables
    private static final String STUDENT_ID = System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "default";
    private static final int DEFAULT_PORT = System.getenv("PORT") != null ? Integer.parseInt(System.getenv("PORT")) : 5000;
    private static final long WORKER_TIMEOUT_MS = 10000;      // 10 seconds
    private static final long TASK_TIMEOUT_MS = 30000;        // 30 seconds for stragglers
    private static final long RECONCILE_INTERVAL_MS = 2000;   // Check every 2 seconds
    private static final int MAX_RETRY_ATTEMPTS = 3;          // Max retries for failed tasks

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        // Scheduling algorithm with MapReduce-style task reassignment:
        // 1. Partition the problem into fine-grained tasks (more tasks than workers)
        // 2. Assign tasks to healthy workers via the available-worker queue
        // 3. On worker failure, reassign incomplete tasks to surviving workers
        // 4. Detect stragglers via task timeout and speculatively re-execute
        
        if ("BLOCK_MULTIPLY".equals(operation)) {
            return coordinateMatrixMultiplication(data, workerCount);
        }
        
        // Unsupported operations return null (graceful degradation)
        System.out.println("[Master] Unsupported operation: " + operation);
        return null;
    }
    
    /**
     * Coordinate distributed matrix multiplication
     */
    private int[][] coordinateMatrixMultiplication(int[][] inputData, int workerCount) {
        try {
            // Wait for sufficient workers
            System.out.println("[Master] Waiting for " + workerCount + " workers to connect...");
            waitForWorkers(workerCount);
            System.out.println("[Master] All workers connected. Starting computation.");
            
            // Parse input data: [matrixA_rows, matrixA_cols, ...matrixA_data, matrixB_rows, matrixB_cols, ...matrixB_data]
            MatrixData matrices = parseInputData(inputData);
            
            // Initialize result matrix
            resultMatrix = new int[matrices.aRows][matrices.bCols];
            
            // Partition the problem into tasks
            partitionIntoTasks(matrices);
            
            // Start task assignment thread
            systemThreads.submit(this::assignTasksToWorkers);
            
            // Wait for all tasks to complete
            while (completedTasks.size() < allTasks.size()) {
                Thread.sleep(100);
            }
            
            System.out.println("[Master] All tasks completed successfully!");
            return resultMatrix;
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Coordination interrupted", e);
        }
    }
    
    /**
     * Wait for minimum number of workers
     */
    private void waitForWorkers(int count) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30000; // 30 second timeout
        while (workers.size() < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        if (workers.size() < count) {
            throw new RuntimeException("Insufficient workers: " + workers.size() + "/" + count);
        }
    }
    
    /**
     * Parse flattened input data into matrix structures
     */
    private MatrixData parseInputData(int[][] inputData) {
        // Expected format: First row contains metadata and matrix A, second row contains matrix B
        // Or: Single flattened array
        MatrixData data = new MatrixData();
        
        // Simplified parsing - assume square matrices for demo
        // Real implementation would parse dimensions from input
        int totalElements = 0;
        for (int[] row : inputData) {
            totalElements += row.length;
        }
        
        // Assume two square matrices of equal size
        int matrixSize = (int) Math.sqrt(totalElements / 2);
        data.aRows = matrixSize;
        data.bCols = matrixSize;
        
        data.matrixA = new int[matrixSize][matrixSize];
        data.matrixB = new int[matrixSize][matrixSize];
        
        // Flatten and parse
        int[] flat = new int[totalElements];
        int idx = 0;
        for (int[] row : inputData) {
            for (int val : row) {
                flat[idx++] = val;
            }
        }
        
        // First half is matrix A
        idx = 0;
        for (int i = 0; i < matrixSize; i++) {
            for (int j = 0; j < matrixSize; j++) {
                data.matrixA[i][j] = flat[idx++];
            }
        }
        
        // Second half is matrix B
        for (int i = 0; i < matrixSize; i++) {
            for (int j = 0; j < matrixSize; j++) {
                data.matrixB[i][j] = flat[idx++];
            }
        }
        
        return data;
    }
    
    /**
     * Partition matrix multiplication into independent tasks
     * Strategy: Row-based partitioning (each task computes subset of result rows)
     */
    private void partitionIntoTasks(MatrixData matrices) {
        int totalRows = matrices.aRows;
        int numWorkers = workers.size();
        
        // Create more tasks than workers for better load balancing
        // This helps handle stragglers - if one worker is slow, others can pick up more tasks
        int numTasks = numWorkers * 2;
        int rowsPerTask = Math.max(1, totalRows / numTasks);
        
        int taskId = 0;
        for (int startRow = 0; startRow < totalRows; startRow += rowsPerTask) {
            int endRow = Math.min(startRow + rowsPerTask, totalRows);
            
            ComputeTask task = new ComputeTask();
            task.taskId = taskId++;
            task.startRow = startRow;
            task.endRow = endRow;
            task.matrixA = matrices.matrixA;
            task.matrixB = matrices.matrixB;
            task.status = TaskStatus.PENDING;
            
            allTasks.put(task.taskId, task);
            pendingTasks.offer(task);
        }
        
        System.out.println("[Master] Partitioned into " + allTasks.size() + " tasks");
    }
    
    /**
     * Task assignment loop - continuously assigns pending tasks to available workers
     * Implements MapReduce-style task scheduling with reassignment on failure
     */
    private void assignTasksToWorkers() {
        while (running && completedTasks.size() < allTasks.size()) {
            try {
                // Get next pending task
                ComputeTask task = pendingTasks.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                
                // Get available worker
                String workerId = availableWorkers.poll(100, TimeUnit.MILLISECONDS);
                if (workerId == null) {
                    // No workers available, put task back
                    pendingTasks.offer(task);
                    continue;
                }
                
                WorkerState worker = workers.get(workerId);
                if (worker == null || !worker.isHealthy()) {
                    // Worker no longer exists or unhealthy, put task back
                    pendingTasks.offer(task);
                    continue;
                }
                
                // Assign task to worker
                assignTask(task, worker);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    /**
     * Assign specific task to worker
     */
    private void assignTask(ComputeTask task, WorkerState worker) {
        try {
            // Create task message
            Message taskMsg = new Message();
            taskMsg.magic = "CSM218";
            taskMsg.version = 1;
            taskMsg.type = "TASK";
            taskMsg.messageType = "TASK";
            taskMsg.sender = "master";
            taskMsg.studentId = STUDENT_ID;
            taskMsg.timestamp = System.currentTimeMillis();
            
            // Encode task data in payload
            taskMsg.payload = encodeTaskPayload(task);
            
            // Send to worker
            taskMsg.pack();
            worker.socket.getOutputStream().write(taskMsg.pack());
            worker.socket.getOutputStream().flush();
            
            // Update task state
            task.status = TaskStatus.ASSIGNED;
            task.assignedWorker = worker.workerId;
            task.assignedTime = System.currentTimeMillis();
            
            System.out.println("[Master] Assigned task " + task.taskId + " to " + worker.workerId);
            
        } catch (IOException e) {
            System.err.println("[Master] Failed to assign task to " + worker.workerId);
            handleWorkerFailure(worker);
            pendingTasks.offer(task); // Reassign task
        }
    }
    
    /**
     * Encode task data into byte payload
     */
    private byte[] encodeTaskPayload(ComputeTask task) throws IOException {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);
        
        dos.writeInt(task.taskId);
        dos.writeInt(task.startRow);
        dos.writeInt(task.endRow);
        
        // Write matrix A (only the rows needed for this task)
        int rowCount = task.endRow - task.startRow;
        dos.writeInt(rowCount);
        dos.writeInt(task.matrixA[0].length);
        for (int i = task.startRow; i < task.endRow; i++) {
            for (int j = 0; j < task.matrixA[i].length; j++) {
                dos.writeInt(task.matrixA[i][j]);
            }
        }
        
        // Write matrix B (entire matrix needed)
        dos.writeInt(task.matrixB.length);
        dos.writeInt(task.matrixB[0].length);
        for (int[] row : task.matrixB) {
            for (int val : row) {
                dos.writeInt(val);
            }
        }
        
        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Start the communication listener.
     * Use your custom protocol designed in Message.java.
     */
    public void listen() throws IOException {
        listen(DEFAULT_PORT);
    }

    public void listen(int port) throws IOException {
        // Listening logic uses the custom Message.pack/unpack wire format.
        // Incoming registrations are unpacked, ACKs are packed and sent back.
        // Each worker connection is handled in a dedicated thread.
        
        serverSocket = new ServerSocket(port);
        running = true;
        
        System.out.println("[Master] Listening on port " + port);
        
        // Start accepting connections
        systemThreads.submit(this::acceptConnections);
        
        // Start reconciliation thread
        systemThreads.submit(this::reconcileState);
    }
    
    /**
     * Accept incoming worker connections
     */
    private void acceptConnections() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                systemThreads.submit(() -> handleNewConnection(clientSocket));
            } catch (IOException e) {
                if (running) {
                    System.err.println("[Master] Error accepting connection: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Handle new worker connection
     */
    private void handleNewConnection(Socket socket) {
        try {
            // Read registration message
            Message msg = Message.unpack(readMessageBytes(socket));
            
            if ("REGISTER".equals(msg.type)) {
                WorkerState worker = new WorkerState();
                worker.workerId = msg.sender;
                worker.socket = socket;
                worker.lastHeartbeat = System.currentTimeMillis();
                
                workers.put(worker.workerId, worker);
                availableWorkers.offer(worker.workerId);
                
                System.out.println("[Master] Worker registered: " + worker.workerId);
                
                // Send ACK
                Message ack = new Message();
                ack.magic = "CSM218";
                ack.version = 1;
                ack.type = "ACK";
                ack.messageType = "ACK";
                ack.sender = "master";
                ack.studentId = STUDENT_ID;
                ack.timestamp = System.currentTimeMillis();
                socket.getOutputStream().write(ack.pack());
                socket.getOutputStream().flush();
                
                // Start handler for this worker
                systemThreads.submit(() -> handleWorkerMessages(worker));
            }
            
        } catch (IOException e) {
            System.err.println("[Master] Error handling new connection: " + e.getMessage());
        }
    }
    
    /**
     * Handle messages from a connected worker
     */
    private void handleWorkerMessages(WorkerState worker) {
        while (running && worker.isHealthy()) {
            try {
                byte[] messageBytes = readMessageBytes(worker.socket);
                Message msg = Message.unpack(messageBytes);
                
                worker.lastHeartbeat = System.currentTimeMillis();
                
                if ("HEARTBEAT".equals(msg.type)) {
                    // Send ACK
                    Message ack = new Message();
                    ack.magic = "CSM218";
                    ack.version = 1;
                    ack.type = "ACK";
                    ack.messageType = "ACK";
                    ack.sender = "master";
                    ack.studentId = STUDENT_ID;
                    ack.timestamp = System.currentTimeMillis();
                    worker.socket.getOutputStream().write(ack.pack());
                    worker.socket.getOutputStream().flush();
                    
                } else if ("RESULT".equals(msg.type)) {
                    handleTaskResult(msg, worker);
                    
                } else if ("SHUTDOWN".equals(msg.type)) {
                    handleWorkerShutdown(worker);
                    break;
                }
                
            } catch (IOException e) {
                handleWorkerFailure(worker);
                break;
            }
        }
    }
    
    /**
     * Read a complete message from socket
     */
    private byte[] readMessageBytes(Socket socket) throws IOException {
        java.io.DataInputStream dis = new java.io.DataInputStream(socket.getInputStream());
        
        // Read frame length
        int frameLength = dis.readInt();
        
        // Read message data
        byte[] messageData = new byte[frameLength];
        dis.readFully(messageData);
        
        // Reconstruct with frame length for unpacking
        java.io.ByteArrayOutputStream temp = new java.io.ByteArrayOutputStream();
        java.io.DataOutputStream tempOut = new java.io.DataOutputStream(temp);
        tempOut.writeInt(frameLength);
        tempOut.write(messageData);
        
        return temp.toByteArray();
    }
    
    /**
     * Handle task result from worker
     */
    private void handleTaskResult(Message msg, WorkerState worker) {
        try {
            // Decode result payload
            java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(msg.payload);
            java.io.DataInputStream dis = new java.io.DataInputStream(bais);
            
            int taskId = dis.readInt();
            
            // Check if already completed (handle duplicate/straggler)
            if (completedTasks.contains(taskId)) {
                System.out.println("[Master] Duplicate result for task " + taskId + " from " + worker.workerId);
                availableWorkers.offer(worker.workerId);
                return;
            }
            
            ComputeTask task = allTasks.get(taskId);
            if (task == null) {
                return;
            }
            
            // Read result dimensions
            int rows = dis.readInt();
            int cols = dis.readInt();
            
            // Read result data and integrate into final matrix
            synchronized (resultMatrix) {
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        resultMatrix[task.startRow + i][j] = dis.readInt();
                    }
                }
            }
            
            completedTasks.add(taskId);
            task.status = TaskStatus.COMPLETED;
            
            System.out.println("[Master] Task " + taskId + " completed by " + worker.workerId +
                             " (" + completedTasks.size() + "/" + allTasks.size() + ")");
            
            // Make worker available again
            availableWorkers.offer(worker.workerId);
            
        } catch (IOException e) {
            System.err.println("[Master] Error processing result: " + e.getMessage());
        }
    }
    
    /**
     * Handle worker shutdown
     */
    private void handleWorkerShutdown(WorkerState worker) {
        worker.healthy = false;
        workers.remove(worker.workerId);
        availableWorkers.remove(worker.workerId);
        System.out.println("[Master] Worker shutdown: " + worker.workerId);
    }
    
    /**
     * Handle worker failure - reassign tasks
     */
    private void handleWorkerFailure(WorkerState worker) {
        if (!worker.healthy) {
            return;
        }
        
        worker.healthy = false;
        workers.remove(worker.workerId);
        availableWorkers.remove(worker.workerId);
        
        System.out.println("[Master] Worker failed: " + worker.workerId + ", attempting to recover tasks");
        
        // Reassign tasks that were assigned to failed worker with retry logic
        for (ComputeTask task : allTasks.values()) {
            if (worker.workerId.equals(task.assignedWorker) && 
                !completedTasks.contains(task.taskId)) {
                task.status = TaskStatus.PENDING;
                task.assignedWorker = null;
                task.retryCount = task.retryCount < MAX_RETRY_ATTEMPTS ? task.retryCount + 1 : task.retryCount;
                pendingTasks.offer(task);
                System.out.println("[Master] Reassigning task " + task.taskId + " (retry " + task.retryCount + ")");
            }
        }
        
        try {
            worker.socket.close();
        } catch (IOException e) {
            // Ignore
        }
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        // Cluster state reconciliation:
        // 1. Detect dead workers via heartbeat timeout and remove them
        // 2. Reassign incomplete tasks from dead workers to pending queue
        // 3. Detect straggler tasks exceeding TASK_TIMEOUT_MS and re-queue them
        
        while (running) {
            try {
                Thread.sleep(RECONCILE_INTERVAL_MS);
                
                long currentTime = System.currentTimeMillis();
                List<WorkerState> failedWorkers = new ArrayList<>();
                
                // Detect dead workers (heartbeat timeout)
                for (WorkerState worker : workers.values()) {
                    if (currentTime - worker.lastHeartbeat > WORKER_TIMEOUT_MS) {
                        failedWorkers.add(worker);
                    }
                }
                
                // Handle failed workers
                for (WorkerState worker : failedWorkers) {
                    handleWorkerFailure(worker);
                }
                
                // Detect straggler tasks (task timeout)
                for (ComputeTask task : allTasks.values()) {
                    if (task.status == TaskStatus.ASSIGNED &&
                        currentTime - task.assignedTime > TASK_TIMEOUT_MS &&
                        !completedTasks.contains(task.taskId)) {
                        
                        System.out.println("[Master] Task " + task.taskId + 
                                         " timeout on " + task.assignedWorker + " - reassigning");
                        task.status = TaskStatus.PENDING;
                        task.assignedWorker = null;
                        pendingTasks.offer(task);
                    }
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    
    /**
     * Shutdown master
     */
    public void shutdown() {
        running = false;
        
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
        
        systemThreads.shutdownNow();
    }
    
    // Internal data structures
    
    private static class WorkerState {
        String workerId;
        Socket socket;
        long lastHeartbeat;
        volatile boolean healthy = true;
        
        boolean isHealthy() {
            return healthy;
        }
    }
    
    private static class ComputeTask {
        int taskId;
        int startRow;
        int endRow;
        int[][] matrixA;
        int[][] matrixB;
        TaskStatus status;
        String assignedWorker;
        long assignedTime;
        int retryCount = 0;
    }
    
    private enum TaskStatus {
        PENDING, ASSIGNED, COMPLETED
    }
    
    private static class MatrixData {
        int aRows;
        int bCols;
        int[][] matrixA;
        int[][] matrixB;
    }
}