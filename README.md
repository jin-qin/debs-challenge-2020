# **DEBS Challenge 2020**

> **Notice**: `Wiki` function is not available for private repo unless we pay to become `Github Pro` user. Therefore, I personally suggest to maintain our documentation by hand, i.e. create a folder inside this repo for documentation, and write them in `Markdown`.

## **Useful infomation**
### [DEBS Challenge 2020 Official Website](https://2020.debs.org/call-for-grand-challenge-solutions/)
### [Official solution example](https://github.com/dmpalyvos/debs-2020-challenge-local)
### [Official grader repo](https://github.com/dmpalyvos/debs-2020-challenge/tree/testing)
### [Sequential clustering-based event detection for Non-Intrusive Load Monitoring](./docs/papers/SIPP2016_Final.pdf) `Paper`
### [DBSCAN](./docs/papers/DBSCAN.pdf) `Paper`

### Dataset
- [Small](https://drive.google.com/file/d/1CjxfsHexbI5T0Ex8onav_CysMpQZPEoJ/view?usp=sharing)
- [Large](https://chalmersuniversity.app.box.com/s/rct6zpzpanmgf8ddpr9x4pn39m17thm7)

### [Rust-based stream processing system](https://github.com/TimelyDataflow/timely-dataflow) (Repository)
### [Naiad: A Timely Dataflow System](http://sigops.org/s/conferences/sosp/2013/papers/p439-murray.pdf) (Paper of Rust-based stream processing system)

## Run in docker

### Quick Run
Run in the top level of this directory: `docker-compose up`

### Rebuild and Run
If you make any changes to the source codes, then do the following steps:
1. run `bash build_docker_image.sh`
2. start grader server
3. run `bash run_solution <host url>`

Example:
`bash run_solution 192.168.0.30`

> **NOTICE:** If yout get error while running `mvn clean package`, try running `sudo rm -rf target`, then commenting `mvn clean package` in `build_docker_image.sh`, run it manually.

## **TO DO**
- [x] Query 1
- [x] Query 2
- [x] Paralleled Query 1 && Query 2
- [x] Fixed watermarks
- [ ] Dynamic watermarks
- [x] Docker stuff (Dockerfile, docker-compose, test with grader etc.)
- [x] Test pass on small dataset
- [x] Test pass on large dataset
- [ ] Lower down latency in Query 1

## **Team info**
### Team members (sorted by first name)
- Gengtao Xu
- Runqi Tian
- Jing Qin

### Mentor
- [Vasiliki (Vasia) Kalavri](https://cs-people.bu.edu/vkalavri)
