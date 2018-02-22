// ThreePhaseCommit.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include <ctime>
#include <iostream>
#include <map>
#include <Windows.h>

#define MSG_SIZE 1
#define COORD_RANK 0

#define MSG_COMMIT_REQ 0
#define MSG_AGREED 1
#define MSG_ABORT 2
#define MSG_PREPARE 3
#define MSG_ACK 4
#define MSG_COMMIT 5
#define MSG_TIMEOUT -1000

#define TIMEOUT 4

#define FAIL_RANK 0

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


const States FAIL_STATE = PRECOMMIT;

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
	for (int i = COORD_RANK + 1; i < NodesCount; i++)
		sendMessage(tag, i);
}

int waitForMessage(int tag, int source, int abortTag, int abortSource)
{
	clock_t await_start = clock();
	std::time_t now;
	double elapsed_secs = 0;
	
	while (elapsed_secs < TIMEOUT)
	{
		if (probeMessage(tag, source))
			return receiveMessage(tag, source);
		if (probeMessage(abortTag, abortSource))
			return receiveMessage(abortTag, abortSource);
		
		now = clock();
		elapsed_secs = double(now - await_start) / CLOCKS_PER_SEC;
	}

	printNodeAndState();
	std::cout << "*** TIMEOUT *** while waiting for <"<< MSG_TAGS_DISPLAY[tag]
		<< "> from #" << source  << std::endl;
	return MSG_TIMEOUT;
}

bool receiveAllAckMessages(int okTag, int refuseTag)
{
	int allCount = NodesCount - 1;
	int ackCount = 0;
	clock_t await_start = clock();
	std::time_t now;
	double elapsed_secs = 0;

	while (elapsed_secs < (TIMEOUT * allCount) && ackCount < allCount)
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
	if (ackCount < allCount)
	{
		printNodeAndState();
		std::cout << "*** TIMEOUT *** while waiting for <" << MSG_TAGS_DISPLAY[okTag]
			<< "> from some cohort" << std::endl;
	}
	return ackCount >= allCount;
}

void coordinatorProgram()
{
	while (true) {
		if (NodeRank == FAIL_RANK && State == FAIL_STATE)
			Sleep((TIMEOUT + 1) * 1000);
		switch (State)
		{
			case QUERY:
			{
				/*printNodeAndState();
				std::cout << "executing state QUERY" << std::endl;*/
				coordBroadcast(MSG_COMMIT_REQ);
				stateTransition(WAITING);
				break;
			}
			case WAITING:
			{
				/*printNodeAndState();
				std::cout << "executing state WAITING" << std::endl;*/
				if (receiveAllAckMessages(MSG_AGREED, MSG_ABORT))
				{
					coordBroadcast(MSG_PREPARE);
					stateTransition(PRECOMMIT);
				}
				else
				{
					coordBroadcast(MSG_ABORT);
					stateTransition(ABORT);
				}
				break;
			}
			case PRECOMMIT:
			{
				/*printNodeAndState();
				std::cout << "executing state PRECOMMIT" << std::endl;*/
				if (receiveAllAckMessages(MSG_ACK, MSG_ABORT))
				{
					coordBroadcast(MSG_COMMIT);
					stateTransition(COMMIT);
				}
				else
				{
					coordBroadcast(MSG_ABORT);
					stateTransition(ABORT);
				}
				break;
			}
			case COMMIT:
			case ABORT:
			{
				/*printNodeAndState();
				std::cout << "executing state ABORT/COMMIT" << std::endl;*/
				printNodeAndState();
				std::cout << "FINISHED" << std::endl;
				return;
			}
		}
	}
}

void cohortProgram()
{
	while (true) {
		if (NodeRank == FAIL_RANK && State == FAIL_STATE)
			Sleep((TIMEOUT + 1) * 1000);
		switch (State)
		{
			case QUERY:
			{
				/*printNodeAndState();
				std::cout << "executing state QUERY" << std::endl;*/
				switch (waitForMessage(MSG_COMMIT_REQ, COORD_RANK, MSG_ABORT, COORD_RANK))
				{
					case MSG_TIMEOUT:
					case MSG_ABORT:
						stateTransition(ABORT);
						break;
					default:
					{
						sendMessage(MSG_AGREED);
						stateTransition(WAITING);
						break;
					}
				}
				break;
			}
			case WAITING:
			{
				/*printNodeAndState();
				std::cout << "executing state WAITING" << std::endl;*/
				switch (waitForMessage(MSG_PREPARE, COORD_RANK, MSG_ABORT, COORD_RANK))
				{
					case MSG_TIMEOUT:
					case MSG_ABORT:
						stateTransition(ABORT);
						break;
					default:
					{
						sendMessage(MSG_ACK);
						stateTransition(PRECOMMIT);
						break;
					}
				}
				break;
			}
			case PRECOMMIT:
			{
				/*printNodeAndState();
				std::cout << "executing state PRECOMMIT" << std::endl;*/
				switch (waitForMessage(MSG_COMMIT, COORD_RANK, MSG_ABORT, COORD_RANK))
				{
					case MSG_ABORT:
						stateTransition(ABORT);
						break;
					default:
					{
						stateTransition(COMMIT);
						break;
					}
				}
				break;
			}
			case COMMIT:
			case ABORT:
			{
				/*printNodeAndState();
				std::cout << "executing state ABORT/COMMIT" << std::endl;*/
				printNodeAndState();
				std::cout << "FINISHED" << std::endl;
				return;
			}
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
	}
	else //if(NodeRank != 1)
	{
		cohortProgram();
	}

	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
    
	return 0;
}

