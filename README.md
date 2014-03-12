# Assignment 4

**Due: Wednesday, March 12th, 2014, 23:59:59 PST**<br/>
**Groups: at most 2 members each**

## Overview

TNI's Tea Emporium has just released their new line of teas! They need to know
what their customers think and, with all their customers being fluent computer
programmers, their ratings are happily submitted over HTTP.

As it stands there are two limitations that need lifting:
 * The existing programmers haven't quite finished implementing the rating
   system correctly; it only stores the most recent rating.
 * The code only stores data on one server.

To complete the project, your job is twofold: 
 * Firstly, the rating of a tea should be determined by the average of combining 
   each rating from a unique customer for that tea; a customer only has one 
   rating for a particular tea, but they're free to change that rating. Each
   person's rating is only counted once, no matter how many times they rate;
   the newest one merely takes precedence.
 * Secondly, as with any booming business, the large number of customers means 
   that not all ratings are going to fit in one database. Use a hash function 
   to shard the data appropriately. 


Helpful hints:
 * Look up the so-called "running average" and how to calculate it,
 * Check out Redis sorted sets to keep track of who's rated and for how much,
 * Have a look through common implementations of hash functions and see if they 
   suite your needs.
 * If you get errors along the lines of "port already in use" make sure you
   don't have any existing instances of python / redis-server running. You can
   check for them with, e.g. "ps aux | grep python".

A virtual machine image which closely matches TNI's Tea Emporium servers has
been released and available as a testing environment for you at the following url: 
http://sfu-innovation.github.io/474-14-1/vm.html

You can, of course, manually test your system by using basic curl commands:

Setting a rating:
```
curl -XPUT -H'Content-type: application/json' -d'{ "rating": 5, "source": "ted" }' http://localhost:2500/rating/maharaja-chai-oolong-tea
```

Getting a rating:
```
curl -XGET http://localhost:2500/rating/maharaja-chai-oolong-tea
```

Removing a rating:
```
curl -XDELETE http://localhost:2500/rating/maharaja-chai-oolong-tea
```

## Testing

A test script has been provided for you, allowing you to evaluate your system. It
can be run as: test/run.py. Nothing in the "test" folder is to be modified.

```
test/run.py --results run.log
test/grade.py --results run.log
```

If you wish to re-run a particular test (e.g. where it says #5US6IYNXYI) you
may do so by passing in the "key" parameter to run. Each key should produce
repeatable results.

```
test/run.py --key 5US6IYNXYI --results run.log
```

## Setup

### Ubuntu

```bash
sudo apt-get install redis-server python python-pip git
sudo pip install -r requirements.txt
```

The redis-server version on Ubuntu below 13.04 is too low to run the test tool.
You can get the latest version of redis-server for these platforms by running
the following commands:

```bash
sudo add-apt-repository ppa:chris-lea/redis-server
sudo apt-get update
sudo apt-get upgrade
```

### Mac OS X

```bash
sudo ruby -e "$(curl -fsSL https://raw.github.com/Homebrew/homebrew/go/install)"
sudo brew install redis python git
sudo pip install -r requirements.txt
```

### Windows

GLHF.
