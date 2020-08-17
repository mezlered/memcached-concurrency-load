# MemcLoad Pytoh 2.7

Script parsing and pours into memcahed the per-minute unloading of the logs of the tracker of installed applications. The key is type and identifiercolon device.

## Installation

```bash
pip install -r requirements.txt
```
## Data to work with

Download files under following links and place them into `./data` directory.

* https://cloud.mail.ru/public/2hZL/Ko9s8R9TA
* https://cloud.mail.ru/public/DzSX/oj8RxGX1A
* https://cloud.mail.ru/public/LoDo/SfsPEzoGc

## Serialization module re-generation (optional)

If you would like to change serialization protocol you should install protobuf 
compiler first:

```bash
brew install protobuf  # macos
snap install protobuf  # snap-enabled linux distro
sudo apt install protobuf-compiler  # debian-based linux distro
```

## Testing

Run the tests:

```bash
python memc_multiprocessing_load.py --test
```