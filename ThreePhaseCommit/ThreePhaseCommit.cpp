// ThreePhaseCommit.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include <ctime>
#include <iostream>
#include <map>

#define MSG_SIZE 1
#define COORD_RANK 0

//#define STATE_QUERY 0
//#define STATE_WAITING 1
//#define STATE_PRECOMMIT 2
//#define STATE_ABORT 3
//#define STATE_COMMIT 4

#define MSG_COMMIT_REQ 0
#define MSG_AGREED 1
#define MSG_ABORT 2
#define MSG_PREPARE 3
#define MSG_ACK 4
#define MSG_COMMIT 5
#define MSG_TIMEOUT -1000

#define TIMEOUT 5


enum States { QUERY, WAITING, PRECOMMIT, ABORT, COMMIT };

std::map<States, const char*> STATES_DISPLAY = {
	{ QUERY, "QUERY" },
	{ WAITING, "WAITING" },
	{ PRECOMMIT, "PRECOMMIT" },
	{ ABORT, "ABORT" },
	{ COMMIT, "COMMIT" },
};

std::map<short, const char*> MSG_TAGS_DISPLAY = {
	{ MSG_COMMIT_REQ, "COMMIT_REQ" },
	{ MSG_AGREED, "AGREED" },
	{ MSG_ABORT, "ABORT" },
	{ MSG_PREPARE, "PREPARE" },
	{ MSG_ACK, "ACK" },
	{ MSG_COMMIT, "COMMIT" }
};


int NodeRank;
int NodesCount;
States State = QUERY;

void printNodeAndState()
{
	std::cout << "#" << NodeRank << " [" << STATES_DISPLAY[State] << "]: ";
}

void stateTransition(States nextState)
{
	printNodeAndState();
	State = nextState;
	std::cout << "state transition => " << STATES_DISPLAY[State] << std::endl;
}

bool probeMessage(int tag = MPI_ANY_TAG, int source = MPI_ANY_SOURCE)
{
	int flagReceived;
	MPI_Status mpiStatus;
	MPI_Iprobe(source, tag, MPI_COMM_WORLD, &flagReceived, &mpiStatus);
	return flagReceived;
}

void sendMessage(int tag, int recipient = COORD_RANK)
{
	MPI_Request request;
	MPI_Isend(&tag, MSG_SIZE, MPI_INT, recipient, tag, MPI_COMM_WORLD, &request);
	printNodeAndState();
	std::cout << MSG_TAGS_DISPLAY[tag] << " sent to #" << recipient << std::endl;
}

int receiveMessage(int tag = MPI_ANY_TAG, int source = MPI_ANY_SOURCE)
{
	int data;
	MPI_Status mpiStatus;
	MPI_Recv(&data, MSG_SIZE, MPI_INT, source, tag, MPI_COMM_WORLD, &mpiStatus);
	printNodeAndState();
	std::cout << MSG_TAGS_DISPLAY[mpiStatus.MPI_TAG] << " received from #" << mpiStatus.MPI_SOURCE << std::endl;
	return mpiStatus.MPI_TAG;
}

void coordBroadcast(int tag)
{
	printNodeAndState();
	std::cout << NodeRank << " "<< NodesCount << std::endl;
	for (int i = COORD_RANK + 1; i < NodesCount; i++)
		sendMessage(tag, i);
}

int waitForMessage(int tag = MPI_ANY_TAG, int source = MPI_ANY_SOURCE)
{
	clock_t await_start = clock();
	std::time_t now;
	double elapsed_secs = 0;
	
	while (elapsed_secs < TIMEOUT)
	{
		if (probeMessage(tag, source))
			return receiveMessage(tag, source);
		
		now = clock();
		elapsed_secs = double(now - await_start) / CLOCKS_PER_SEC;
	}

	return MSG_TIMEOUT;
}

bool receiveAllAckMessages(int okTag, int refuseTag)
{
	int allCount = NodesCount - 1;
	int ackCount = 0;
	clock_t await_start = clock();
	std::time_t now;
	double elapsed_secs = 0;

	while (elapsed_secs < TIMEOUT && ackCount < allCount)
	{
		while (probeMessage(okTag))
		{
			receiveMessage(okTag);
			ackCount++;
		}

		if (probeMessage(refuseTag))
		{
			receiveMessage(refuseTag);
			return false;
		}

		now = clock();
		elapsed_secs = double(now - await_start) / CLOCKS_PER_SEC;
	}

	return ackCount >= allCount;
}

void coordinatorProgram()
{
	while (true) {
		switch (State)
		{
			case QUERY:
			{
				coordBroadcast(MSG_COMMIT_REQ);
				stateTransition(WAITING);
			}
			case WAITING:
			{
				if (receiveAllAckMessages(MSG_AGREED, MSG_ABORT))
				{
					coordBroadcast(MSG_PREPARE);
					stateTransition(PRECOMMIT);
				}
				else
				{
					coordBroadcast(MSG_ABORT);
					stateTransition(PRECOMMIT);
				}
			}
			case PRECOMMIT:
			case COMMIT:
			case ABORT:
				return;
		}
	}
}

void cohortProgram()
{
	while (true) {
		switch (State)
		{
			case QUERY:
			{
				if (waitForMessage(MSG_COMMIT_REQ, COORD_RANK) == MSG_TIMEOUT)
				{
					stateTransition(ABORT);
				}
				else
				{
					sendMessage(MSG_AGREED);
					stateTransition(WAITING);
				}
			}
			case WAITING:
			case PRECOMMIT:
			case COMMIT:
			case ABORT:
				return;
		}
	}
}

int main(int argc, char* argv[])
{
	MPI_Init(&argc, &argv);

	MPI_Comm_rank(MPI_COMM_WORLD, &NodeRank);
	MPI_Comm_size(MPI_COMM_WORLD, &NodesCount);
	MPI_Barrier(MPI_COMM_WORLD);
	if (NodeRank == COORD_RANK)
	{
		coordinatorProgram();
		/*MPI_Request request;
		int tag = MSG_COMMIT_REQ;
		MPI_Isend(&tag, MSG_SIZE, MPI_INT, 1, MSG_COMMIT_REQ, MPI_COMM_WORLD, &request);*/

	}
	else
	{
		cohortProgram();
		/*MPI_Request request;
		int tag = MSG_COMMIT_REQ;
		MPI_Isend(&tag, MSG_SIZE, MPI_INT, 0, MSG_COMMIT_REQ, MPI_COMM_WORLD, &request);*/
		/*std::time_t result = std::time(nullptr);

		char helloStr[12];
		MPI_Recv(helloStr, _countof(helloStr), MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		printf("Rank 1 received string %s from Rank 0\n", helloStr);*/
		//int data;
		//MPI_Recv(&data, MSG_SIZE, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	}
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
    return 0;
}

