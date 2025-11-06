package CloudsimExamplePackage;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

import java.io.*;
import java.io.File;
import java.util.*;

public class ExampleClass {

    // ====== Konfigurasi Sistem (Berdasarkan Paper) ======
    // Paper menggunakan multiple datacenters dengan hosts yang memiliki VM
    static final int DATACENTER_COUNT = 6;  // P datacenters (sesuai paper Section 3.1)
    static final int HOST_PER_DATACENTER = 3;  // k hosts per datacenter
    static final int VM_PER_HOST = 3;  // m VMs per host
    static final int PES_PER_VM = 1;  // Number of cores per VM

    // DBO Algorithm Parameters
    static final int POPULATION = 30;
    static final int MAX_ITER = 200;
    static final double PROB_LOCAL = 0.7;

    static Random rng = new Random(42);

    // ====== Path dataset ======(Change This Part as Needed)
    static final String DATASET_PATH = "DatasetCloudProvisioning/datasets/randomSimple/randSimple1000.txt";
    
    public static void main(String[] args) {
        System.out.println("=== CloudSim Simulation with Round Robin Scheduling ===");
        System.out.println("=== Configuration based on EASA-MORU Paper ===\n");

        Scanner sc = new Scanner(System.in);
        System.out.println("Pilih mode simulasi:");
        System.out.println("1. No Scheduling (Default Broker)");
        System.out.println("2. Round Robin Scheduling");
        System.out.println("3. DBO Scheduling (EASA-MORU)");
        System.out.print("Masukkan pilihan (1/2/3): ");
        int choice = sc.nextInt();

        String schedulingMode = "No Scheduling";
        if (choice == 2) {
            schedulingMode = "Round Robin";
        } else if (choice == 3) {
            schedulingMode = "DBO (EASA-MORU)";
        }

        List<double[]> results = new ArrayList<>();

        for (int run = 1; run <= 10; run++) {
            System.out.printf("%n========== SIMULATION RUN %d (%s) ==========%n", run, schedulingMode);
            double[] metrics = runSimulation(choice);
            results.add(metrics);
        }

        printSummary(results, schedulingMode);
    }

    // ====================================================================
    // ================== SIMULATION RUN ==================================
    // ====================================================================

    private static double[] runSimulation(int schedulingChoice) {
        try {
            CloudSim.init(1, Calendar.getInstance(), false);

            // ==== Buat Datacenter ====
            List<Datacenter> datacenters = new ArrayList<>();
            for (int i = 0; i < DATACENTER_COUNT; i++) {
                datacenters.add(createDatacenter("Datacenter_" + i, HOST_PER_DATACENTER));
            }

            DatacenterBroker broker = createBroker();
            int brokerId = broker.getId();

            // ==== Buat VM ====
            int totalVMs = DATACENTER_COUNT * HOST_PER_DATACENTER * VM_PER_HOST;
            List<Vm> vmList = createVMList(brokerId, totalVMs);
            broker.submitVmList(vmList);

            // ==== Load Cloudlets dari dataset ====
            List<Cloudlet> cloudletList = loadCloudletsFromDataset(brokerId, PES_PER_VM, DATASET_PATH);

            System.out.printf("Configuration: %d Datacenters, %d Hosts, %d VMs, %d Cloudlets%n",
                    DATACENTER_COUNT, DATACENTER_COUNT * HOST_PER_DATACENTER, totalVMs, cloudletList.size());

            // ==== Scheduling berdasarkan pilihan ====
            if (schedulingChoice == 2) {
                System.out.println("Menjalankan Round Robin Scheduling...");
                Map<Integer, Integer> assignment = RoundRobinScheduling(cloudletList, vmList);
                for (Cloudlet c : cloudletList) {
                    c.setVmId(assignment.get(c.getCloudletId()));
                }
            } else if (schedulingChoice == 3) {
                System.out.println("Menjalankan DBO Scheduling (EASA-MORU)...");
                Map<Integer, Integer> assignment = DBOAlgorithm(cloudletList, vmList);
                for (Cloudlet c : cloudletList) {
                    c.setVmId(assignment.get(c.getCloudletId()));
                }
            }

            broker.submitCloudletList(cloudletList);

            CloudSim.startSimulation();
            List<Cloudlet> finished = broker.getCloudletReceivedList();
            CloudSim.stopSimulation();

            return computeMetrics(finished, vmList);

        } catch (Exception e) {
            e.printStackTrace();
            return new double[0];
        }
    }

    // ====================================================================
    // ================== INFRASTRUKTUR CLOUDSIM ==========================
    // ====================================================================

    private static Datacenter createDatacenter(String name, int numHosts) {
        List<Host> hostList = new ArrayList<>();
        
        // Berdasarkan paper: Host memiliki MIPS, RAM, Bandwidth, Storage
        for (int i = 0; i < numHosts; i++) {
            // Host specifications based on paper Section 3.1
            // PE (Processing Element) dengan MIPS capability
            List<Pe> peList = new ArrayList<>();
            int pesPerHost = 4;  // Multiple cores per host
            int hostMips = 5000;  // Million Instructions Per Second
            
            for (int j = 0; j < pesPerHost; j++) {
                peList.add(new Pe(j, new PeProvisionerSimple(hostMips)));
            }

            Host host = new Host(
                    i,
                    new RamProvisionerSimple(8192),      // 8GB RAM (vm_mem in paper)
                    new BwProvisionerSimple(10000),       // 10 Gbps bandwidth
                    1000000,                              // 1TB storage (vm_st in paper)
                    peList,
                    new VmSchedulerTimeShared(peList)
            );
            hostList.add(host);
        }

        // Datacenter characteristics (Section 3.1)
        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                "x86",              // Architecture
                "Linux",            // OS
                "Xen",              // VMM
                hostList,           // Host list
                10.0,               // Time zone
                3.0,                // Cost per second (varying electricity price in paper)
                0.05,               // Cost per memory
                0.1,                // Cost per storage
                0.1                 // Cost per bandwidth
        );

        try {
            return new Datacenter(name, characteristics,
                    new VmAllocationPolicySimple(hostList),
                    new LinkedList<>(), 0);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static DatacenterBroker createBroker() {
        try {
            return new DatacenterBroker("Broker");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static List<Vm> createVMList(int brokerId, int count) {
        List<Vm> list = new ArrayList<>();
        
        // VM specifications based on paper Section 3.1
        // vm_mips: processing power in MIPS
        // vm_cpus: number of cores
        // vm_mem: memory
        // vm_st: storage
        
        for (int i = 0; i < count; i++) {
            // Heterogeneous VMs with different MIPS (500-2000 range)
            int vmMips = 500 + rng.nextInt(1500);  // Random MIPS for heterogeneity
            int vmCpus = PES_PER_VM;                // vm_cpus from paper
            int vmRam = 512;                        // vm_mem (512 MB - 2GB range)
            long vmBw = 1000;                       // Bandwidth
            long vmSize = 10000;                    // vm_st (storage)
            
            Vm vm = new Vm(
                    i,                              // VM ID
                    brokerId,                       // Broker ID
                    vmMips,                         // MIPS
                    vmCpus,                         // Number of CPUs
                    vmRam,                          // RAM
                    vmBw,                           // Bandwidth
                    vmSize,                         // Storage
                    "Xen",                          // VMM
                    new CloudletSchedulerTimeShared()
            );
            list.add(vm);
        }
        return list;
    }

    // ====================================================================
    // ================== PEMBACAAN DATASET ===============================
    // ====================================================================

    // Helper method to parse decimal or integer values to long
    private static long parseToLong(String value) {
        try {
            // Try parsing as double first, then convert to long
            double doubleValue = Double.parseDouble(value);
            return (long) Math.round(doubleValue);
        } catch (NumberFormatException e) {
            // If all else fails, return default
            return 1000;
        }
    }

    private static List<Cloudlet> loadCloudletsFromDataset(int brokerId, int pesNumber, String datasetPath) {
        File file = new File(datasetPath);
        List<Cloudlet> list = new ArrayList<>();
        UtilizationModel model = new UtilizationModelFull();

        if (!file.exists()) {
            System.out.println("⚠️ Dataset tidak ditemukan. Membuat cloudlet acak...");
            // Cloudlet specifications based on paper Section 3.1
            // Cl_i: cloudlet dengan length dalam Million Instructions (MI)
            for (int i = 0; i < 100; i++) {
                long length = 5000 + rng.nextInt(15000);  // Length in MI (Cl_i total)
                long fileSize = 300;                       // Input file size
                long outputSize = 300;                     // Output file size
                
                Cloudlet c = new Cloudlet(i, length, pesNumber, fileSize, outputSize, 
                                         model, model, model);
                c.setUserId(brokerId);
                list.add(c);
            }
            return list;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            int id = 0;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;  // Skip empty and comment lines
                
                try {
                    String[] parts = line.split("[,;\\s]+");
                    
                    // Parse with support for decimal values (convert to long)
                    long length = parseToLong(parts[0]);           // Cloudlet length (MI)
                    long fileSize = (parts.length > 1) ? parseToLong(parts[1]) : 300;
                    long outputSize = (parts.length > 2) ? parseToLong(parts[2]) : 300;
                    
                    Cloudlet c = new Cloudlet(id, length, pesNumber, fileSize, outputSize, 
                                             model, model, model);
                    c.setUserId(brokerId);
                    list.add(c);
                    id++;
                } catch (NumberFormatException e) {
                    System.err.println("⚠️ Skipping invalid line: " + line);
                    continue;
                }
            }
            System.out.println("✓ Loaded " + list.size() + " cloudlets from dataset");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    // ====================================================================
    // ================== ROUND ROBIN SCHEDULING ==========================
    // ====================================================================

    private static Map<Integer, Integer> RoundRobinScheduling(List<Cloudlet> cloudlets, List<Vm> vms) {
        Map<Integer, Integer> assignment = new HashMap<>();
        int vmCount = vms.size();
        
        // Distribusi cloudlet secara merata ke VM menggunakan Round Robin
        for (int i = 0; i < cloudlets.size(); i++) {
            int vmIndex = i % vmCount;
            int vmId = vms.get(vmIndex).getId();
            assignment.put(cloudlets.get(i).getCloudletId(), vmId);
        }
        
        System.out.printf("Round Robin: %d cloudlets → %d VMs (cyclic assignment)%n", 
                         cloudlets.size(), vmCount);
        
        return assignment;
    }

    // ====================================================================
    // ================== DBO ALGORITHM (EASA-MORU) =======================
    // ====================================================================

    private static Map<Integer, Integer> DBOAlgorithm(List<Cloudlet> cloudlets, List<Vm> vms) {
        int nCloud = cloudlets.size();
        int nVm = vms.size();

        // Initialize population
        List<int[]> population = new ArrayList<>();
        for (int i = 0; i < POPULATION; i++) {
            int[] assign = new int[nCloud];
            for (int j = 0; j < nCloud; j++) assign[j] = rng.nextInt(nVm);
            population.add(assign);
        }

        // Fitness function: minimize makespan (Equation 1 from paper)
        // ET(Cl_i, vm_j) = Cl_i_total / total_mips(vm_j)
        java.util.function.Function<int[], Double> fitness = (assign) -> {
            double[] vmLoad = new double[nVm];
            for (int i = 0; i < nCloud; i++) {
                int vmIdx = assign[i];
                double execTime = (double) cloudlets.get(i).getCloudletLength() / 
                                 (vms.get(vmIdx).getMips() * vms.get(vmIdx).getNumberOfPes());
                vmLoad[vmIdx] += execTime;
            }
            // Makespan = maximum completion time
            double max = 0;
            for (double v : vmLoad) max = Math.max(max, v);
            return max;
        };

        // Find initial best solution
        double bestFitness = Double.MAX_VALUE;
        int[] best = null;
        for (int[] ind : population) {
            double f = fitness.apply(ind);
            if (f < bestFitness) {
                bestFitness = f;
                best = ind.clone();
            }
        }

        // DBO iterations (Algorithm 1 from paper)
        for (int iter = 0; iter < MAX_ITER; iter++) {
            for (int[] ind : population) {
                int[] newInd = ind.clone();
                for (int k = 0; k < nCloud; k++) {
                    // Ball-rolling and breeding behavior
                    if (rng.nextDouble() < 0.2) {
                        newInd[k] = best[k];  // Follow best solution
                    } else if (rng.nextDouble() < PROB_LOCAL) {
                        newInd[k] = rng.nextInt(nVm);  // Local search
                    }
                }
                double newFit = fitness.apply(newInd);
                double oldFit = fitness.apply(ind);
                if (newFit < oldFit) System.arraycopy(newInd, 0, ind, 0, nCloud);
            }

            // Update best solution
            for (int[] ind : population) {
                double f = fitness.apply(ind);
                if (f < bestFitness) {
                    bestFitness = f;
                    best = ind.clone();
                }
            }
        }

        Map<Integer, Integer> result = new HashMap<>();
        for (int i = 0; i < nCloud; i++) {
            result.put(cloudlets.get(i).getCloudletId(), best[i]);
        }
        
        System.out.printf("DBO: Optimized makespan = %.2f%n", bestFitness);
        return result;
    }

    // ====================================================================
    // ================== METRIK SIMULASI (Paper Section 4) ===============
    // ====================================================================

    private static double[] computeMetrics(List<Cloudlet> list, List<Vm> vms) {
        double totalCpu = 0, totalWait = 0, avgStart = 0, avgExec = 0, avgFinish = 0;
        double makeSpan = 0;
        int finished = list.size();

        for (Cloudlet c : list) {
            totalCpu += c.getActualCPUTime();
            totalWait += c.getWaitingTime();
            avgStart += c.getExecStartTime();
            avgExec += c.getActualCPUTime();
            avgFinish += c.getFinishTime();
            makeSpan = Math.max(makeSpan, c.getFinishTime());
        }

        avgStart /= finished;
        avgExec /= finished;
        avgFinish /= finished;

        // Metrics from paper
        double throughput = finished / makeSpan;                    // Tasks per unit time
        double imbalance = avgFinish - avgExec;                     // Degree of imbalance
        double utilization = (totalCpu / (vms.size() * makeSpan)) * 100;  // Resource utilization
        double energy = makeSpan * 0.000277;                        // Energy consumption (kWh)

        System.out.printf("Metrics: CPU=%.2f | Wait=%.2f | Makespan=%.2f | Throughput=%.4f | Util=%.2f%%\n",
                totalCpu, totalWait, makeSpan, throughput, utilization);

        return new double[]{totalCpu, totalWait, avgStart, avgExec, avgFinish, 
                           throughput, makeSpan, imbalance, utilization, energy};
    }

    // ====================================================================
    // ================== PRINT SUMMARY ===================================
    // ====================================================================

    private static void printSummary(List<double[]> results, String schedulingMode) {
        System.out.println("\n" + "=".repeat(150));
        System.out.println("SUMMARY (10 RUNS) - " + schedulingMode);
        System.out.println("=".repeat(150));
        
        // Human-readable format
        System.out.printf("%-5s %-12s %-12s %-12s %-12s %-12s %-12s %-12s %-12s %-12s %-12s%n",
                "Run", "TotalCPU", "TotalWait", "AvgStart", "AvgExec", "AvgFinish", 
                "Throughput", "Makespan", "Imbalance", "Util(%)", "Energy(kWh)");
        System.out.println("-".repeat(150));

        double[] mean = new double[10];
        for (int i = 0; i < results.size(); i++) {
            double[] r = results.get(i);
            System.out.printf("%-5d %-12.2f %-12.2f %-12.2f %-12.2f %-12.2f %-12.4f %-12.2f %-12.2f %-12.2f %-12.6f%n",
                    (i + 1), r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]);
            for (int j = 0; j < 10; j++) mean[j] += r[j];
        }
        for (int j = 0; j < 10; j++) mean[j] /= results.size();

        System.out.println("=".repeat(150));
        System.out.printf("%-5s %-12.2f %-12.2f %-12.2f %-12.2f %-12.2f %-12.4f %-12.2f %-12.2f %-12.2f %-12.6f%n",
                "MEAN", mean[0], mean[1], mean[2], mean[3], mean[4], mean[5], mean[6], mean[7], mean[8], mean[9]);
        System.out.println("=".repeat(150));
        
        // Excel-ready format with TAB delimiter
        System.out.println("\n\n========== EXCEL COPY-PASTE FORMAT (TAB DELIMITED) ==========");
        System.out.println("Copy the lines below and paste directly into Excel:\n");
        
        // Header row
        System.out.println("Run\tTotal CPU Time\tTotal Wait Time\tAverage Start Time\tAverage Execution Time\t" +
                          "Average Finish Time\tThroughput\tMakespan\tImbalance Degree\tResource Utilization(%)\t" +
                          "Total Energy Consumption(kWh)");
        
        // Data rows
        for (int i = 0; i < results.size(); i++) {
            double[] r = results.get(i);
            System.out.printf("%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.4f\t%.2f\t%.2f\t%.2f\t%.6f%n",
                    (i + 1), r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]);
        }
        
        // Mean row
        System.out.printf("MEAN\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.4f\t%.2f\t%.2f\t%.2f\t%.6f%n",
                mean[0], mean[1], mean[2], mean[3], mean[4], mean[5], mean[6], mean[7], mean[8], mean[9]);
        
        System.out.println("\n=============================================================");
    }

}
