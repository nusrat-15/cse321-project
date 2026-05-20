#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define max_process 64
#define max_children 64
#define MAX_LINE 256

#define RUNNING 0
#define BLOCKED 1
#define ZOMBIE 2

typedef struct process_control_block {
    int process_id;
    int parent_process_id;
    int process_state;
    int exit_status;
    int child_process_ids[max_children];
    int child_count;
    pthread_cond_t child_exit_condition;
} process_control_block;

typedef struct {
    process_control_block *process_table[max_process];
    int active_process_count;
    int next_process_id;
    pthread_mutex_t table_lock;
} process_manager;

typedef struct {
    int thread_id;
    const char *script_filename;
} worker_thread_arg;

typedef struct snapshot_queue_node {
    char *snapshot_text;
    struct snapshot_queue_node *next_node;
} snapshot_queue_node;

static process_manager global_process_manager;

static pthread_cond_t monitor_condition = PTHREAD_COND_INITIALIZER;
static snapshot_queue_node *snapshot_queue_head = NULL;
static snapshot_queue_node *snapshot_queue_tail = NULL;
static int should_stop_monitor = 0;
static FILE *snapshot_output_file = NULL;

static __thread int current_worker_thread_id = -1;

const char *get_state_name(int process_state) {
    switch (process_state) {
        case RUNNING: return "RUNNING";
        case BLOCKED: return "BLOCKED";
        case ZOMBIE:  return "ZOMBIE";
        default:      return "UNKNOWN";
    }
}

process_control_block *create_process(int process_id, int parent_process_id) {
    process_control_block *new_process =
        (process_control_block *)calloc(1, sizeof(process_control_block));
    if (new_process == NULL) {
        return NULL;
    }

    (*new_process).process_id = process_id;
    (*new_process).parent_process_id = parent_process_id;
    (*new_process).process_state = RUNNING;
    (*new_process).exit_status = 0;
    (*new_process).child_count = 0;

    if (pthread_cond_init(&new_process->child_exit_condition, NULL) != 0) {
        free(new_process);
        return NULL;
    }

    return new_process;
}

void destroy_process(process_control_block *process) {
    if (process == NULL) {
        return;
    }

    pthread_cond_destroy(&process->child_exit_condition);
    free(process);
}

process_control_block *find_process_by_id(int process_id) {
    for (int table_index = 0; table_index < max_process; table_index++) {
        if (global_process_manager.process_table[table_index] != NULL && global_process_manager.process_table[table_index]->process_id == process_id) {
            return global_process_manager.process_table[table_index];
        }
    }
    return NULL;
}

int find_process_table_index(int process_id) {
    for (int table_index = 0; table_index < max_process; table_index++) {
        if (global_process_manager.process_table[table_index] != NULL && global_process_manager.process_table[table_index]->process_id == process_id) {
            return table_index;
        }
    }
    return -1;
}

bool insert_process(process_control_block *process) {
    if (process == NULL || global_process_manager.active_process_count >= max_process) {
        return false;
    }

    for (int table_index = 0; table_index < max_process; table_index++) {
        if (global_process_manager.process_table[table_index] == NULL) {
            global_process_manager.process_table[table_index] = process;
            global_process_manager.active_process_count++;
            return true;
        }
    }

    return false;
}

process_control_block *remove_process_by_id(int process_id) {
    int table_index = find_process_table_index(process_id);
    if (table_index == -1) {
        return NULL;
    }

    process_control_block *removed_process =
        global_process_manager.process_table[table_index];
    global_process_manager.process_table[table_index] = NULL;
    global_process_manager.active_process_count--;

    return removed_process;
}

bool parent_has_child(process_control_block *parent_process, int child_process_id) {
    if (parent_process == NULL) {
        return false;
    }

    for (int child_index = 0; child_index < parent_process->child_count; child_index++) {
        if (parent_process->child_process_ids[child_index] == child_process_id) {
            return true;
        }
    }

    return false;
}

bool append_child_to_parent(process_control_block *parent_process, int child_process_id) {
    if (parent_process == NULL || parent_process->child_count >= max_children) {
        return false;
    }

    if (parent_has_child(parent_process, child_process_id)) {
        return true;
    }

    parent_process->child_process_ids[parent_process->child_count] = child_process_id;
    parent_process->child_count++;
    return true;
}

void detach_child_from_parent(process_control_block *parent_process, int child_process_id) {
    if (parent_process == NULL) {
        return;
    }

    for (int child_index = 0; child_index < parent_process->child_count; child_index++) {
        if (parent_process->child_process_ids[child_index] == child_process_id) {
            for (int shift_index = child_index;
                 shift_index < parent_process->child_count - 1;
                 shift_index++) {
                parent_process->child_process_ids[shift_index] =
                    parent_process->child_process_ids[shift_index + 1];
            }
            parent_process->child_count--;
            return;
        }
    }
}

bool parent_can_wait_for_child(process_control_block *parent_process, int child_process_id) {
    if (parent_process == NULL) {
        return false;
    }

    if (child_process_id == -1) {
        return parent_process->child_count > 0;
    }

    return parent_has_child(parent_process, child_process_id);
}

 process_control_block *find_zombie_child(
    process_control_block *parent_process,
    int child_process_id
) {
    if (parent_process == NULL) {
        return NULL;
    }

    for (int child_index = 0; child_index < parent_process->child_count; child_index++) {
        int current_child_process_id = parent_process->child_process_ids[child_index];

        if (child_process_id != -1 && current_child_process_id != child_process_id) {
            continue;
        }

        process_control_block *child_process =
            find_process_by_id(current_child_process_id);
        if (child_process != NULL && child_process->process_state == ZOMBIE) {
            return child_process;
        }
    }

    return NULL;
}

void reparent_children_to_init(process_control_block *process) {
    if (process == NULL || process->process_id == 1 || process->child_count == 0) {
        return;
    }

    process_control_block *init_process = find_process_by_id(1);
    if (init_process == NULL) {
        return;
    }

    for (int child_index = 0; child_index < process->child_count; child_index++) {
        int child_process_id = process->child_process_ids[child_index];
        process_control_block *child_process = find_process_by_id(child_process_id);

        if (child_process != NULL) {
            child_process->parent_process_id = 1;
            append_child_to_parent(init_process, child_process_id);
        }
    }

    process->child_count = 0;
}

void build_sorted_process_list(
    process_control_block **sorted_process_list,
    int *process_count
) {
    int collected_count = 0;

    for (int table_index = 0; table_index < max_process; table_index++) {
        if (global_process_manager.process_table[table_index] != NULL) {
            sorted_process_list[collected_count++] =
                global_process_manager.process_table[table_index];
        }
    }

    for (int left_index = 0; left_index < collected_count; left_index++) {
        for (int right_index = left_index + 1;
             right_index < collected_count;
             right_index++) {
            if (sorted_process_list[right_index]->process_id <
                sorted_process_list[left_index]->process_id) {
                process_control_block *temporary_process =
                    sorted_process_list[left_index];
                sorted_process_list[left_index] =
                    sorted_process_list[right_index];
                sorted_process_list[right_index] = temporary_process;
            }
        }
    }

    *process_count = collected_count;
}

char *create_process_table_text(void) {
    process_control_block *sorted_process_list[max_process];
    int process_count = 0;

    build_sorted_process_list(sorted_process_list, &process_count);

    int buffer_size = 4096;
    char *output_buffer = (char *)malloc(buffer_size);
    if (output_buffer == NULL) {
        return NULL;
    }

    int used_length = 0;

    used_length += snprintf(output_buffer + used_length, buffer_size - used_length,
                            "PID\tPPID\tSTATE\t\tEXIT_STATUS\n");
    used_length += snprintf(output_buffer + used_length, buffer_size - used_length,
                            "-----------------------------------------------\n");

    for (int process_index = 0; process_index < process_count; process_index++) {
        process_control_block *current_process = sorted_process_list[process_index];

        if (current_process->process_state == ZOMBIE) {
            used_length += snprintf(output_buffer + used_length, buffer_size - used_length,
                                    "%d\t%d\t%-8s\t%d\n",
                                    current_process->process_id,
                                    current_process->parent_process_id,
                                    get_state_name(current_process->process_state),
                                    current_process->exit_status);
        } else {
            used_length += snprintf(output_buffer + used_length, buffer_size - used_length,
                                    "%d\t%d\t%-8s\t-\n",
                                    current_process->process_id,
                                    current_process->parent_process_id,
                                    get_state_name(current_process->process_state));
        }

        if (used_length > buffer_size - 128) {
            buffer_size *= 2;
            char *resized_buffer = (char *)realloc(output_buffer, buffer_size);
            if (resized_buffer == NULL) {
                free(output_buffer);
                return NULL;
            }
            output_buffer = resized_buffer;
        }
    }

    return output_buffer;
}

void enqueue_snapshot(const char *snapshot_title) {
    char *process_table_text = create_process_table_text();
    if (process_table_text == NULL) {
        return;
    }

    int required_length =
        (int)strlen(snapshot_title) + (int)strlen(process_table_text) + 4;
    char *full_snapshot_text = (char *)malloc(required_length);
    if (full_snapshot_text == NULL) {
        free(process_table_text);
        return;
    }

    snprintf(full_snapshot_text, required_length, "%s\n%s\n",
             snapshot_title, process_table_text);
    free(process_table_text);

    snapshot_queue_node *new_snapshot_node =
        (snapshot_queue_node *)malloc(sizeof(snapshot_queue_node));
    if (new_snapshot_node == NULL) {
        free(full_snapshot_text);
        return;
    }

    new_snapshot_node->snapshot_text = full_snapshot_text;
    new_snapshot_node->next_node = NULL;

    if (snapshot_queue_tail == NULL) {
        snapshot_queue_head = new_snapshot_node;
        snapshot_queue_tail = new_snapshot_node;
    } else {
        snapshot_queue_tail->next_node = new_snapshot_node;
        snapshot_queue_tail = new_snapshot_node;
    }

    pthread_cond_signal(&monitor_condition);
}

void log_process_snapshot(const char *action_name, int first_value, int second_value) {
    char snapshot_title[256];

    if (second_value == -99999) {
        snprintf(snapshot_title, sizeof(snapshot_title),
                 "Thread %d calls %s %d",
                 current_worker_thread_id, action_name, first_value);
    } else if (first_value == -99999) {
        snprintf(snapshot_title, sizeof(snapshot_title),
                 "Thread %d calls %s",
                 current_worker_thread_id, action_name);
    } else {
        snprintf(snapshot_title, sizeof(snapshot_title),
                 "Thread %d calls %s %d %d",
                 current_worker_thread_id, action_name, first_value, second_value);
    }

    enqueue_snapshot(snapshot_title);
}

void *monitor_thread(void *thread_arg) {
    (void)thread_arg;

    while (1) {
        pthread_mutex_lock(&global_process_manager.table_lock);

        while (snapshot_queue_head == NULL && should_stop_monitor == 0) {
            pthread_cond_wait(&monitor_condition, &global_process_manager.table_lock);
        }

        if (snapshot_queue_head == NULL && should_stop_monitor == 1) {
            pthread_mutex_unlock(&global_process_manager.table_lock);
            break;
        }

        snapshot_queue_node *current_snapshot_node = snapshot_queue_head;
        snapshot_queue_head = current_snapshot_node->next_node;

        if (snapshot_queue_head == NULL) {
            snapshot_queue_tail = NULL;
        }

        pthread_mutex_unlock(&global_process_manager.table_lock);

        fputs(current_snapshot_node->snapshot_text, snapshot_output_file);
        fflush(snapshot_output_file);

        free(current_snapshot_node->snapshot_text);
        free(current_snapshot_node);
    }

    return NULL;
}

/* ---------------------- main process manager functions ---------------------- */

void pm_init(void) {
    memset(&global_process_manager, 0, sizeof(global_process_manager));
    global_process_manager.next_process_id = 2;

    if (pthread_mutex_init(&global_process_manager.table_lock, NULL) != 0) {
        fprintf(stderr, "mutex initialization failed\n");
        exit(EXIT_FAILURE);
    }

    process_control_block *init_process = create_process(1, 0);
    if (init_process == NULL) {
        fprintf(stderr, "failed to create initial process\n");
        exit(EXIT_FAILURE);
    }

    pthread_mutex_lock(&global_process_manager.table_lock);

    if (!insert_process(init_process)) {
        pthread_mutex_unlock(&global_process_manager.table_lock);
        destroy_process(init_process);
        fprintf(stderr, "failed to insert initial process into process table\n");
        exit(EXIT_FAILURE);
    }

    enqueue_snapshot("Initial Process Table");

    pthread_mutex_unlock(&global_process_manager.table_lock);
}

void pm_destroy(void) {
    pthread_mutex_lock(&global_process_manager.table_lock);

    for (int table_index = 0; table_index < max_process; table_index++) {
        if (global_process_manager.process_table[table_index] != NULL) {
            destroy_process(global_process_manager.process_table[table_index]);
            global_process_manager.process_table[table_index] = NULL;
        }
    }

    global_process_manager.active_process_count = 0;

    pthread_mutex_unlock(&global_process_manager.table_lock);
    pthread_mutex_destroy(&global_process_manager.table_lock);
}

int pm_fork(int parent_process_id) {
    pthread_mutex_lock(&global_process_manager.table_lock);

    process_control_block *parent_process = find_process_by_id(parent_process_id);

    if (parent_process == NULL || parent_process->process_state == ZOMBIE) {
        pthread_mutex_unlock(&global_process_manager.table_lock);
        return -1;
    }

    if (global_process_manager.active_process_count >= max_process) {
        pthread_mutex_unlock(&global_process_manager.table_lock);
        return -1;
    }

    int child_process_id = global_process_manager.next_process_id++;
    process_control_block *child_process =
        create_process(child_process_id, parent_process_id);

    if (child_process == NULL) {
        pthread_mutex_unlock(&global_process_manager.table_lock);
        return -1;
    }

    if (!insert_process(child_process)) {
        destroy_process(child_process);
        pthread_mutex_unlock(&global_process_manager.table_lock);
        return -1;
    }

    if (!append_child_to_parent(parent_process, child_process_id)) {
        remove_process_by_id(child_process_id);
        destroy_process(child_process);
        pthread_mutex_unlock(&global_process_manager.table_lock);
        return -1;
    }

    log_process_snapshot("pm_fork", parent_process_id, -99999);

    pthread_mutex_unlock(&global_process_manager.table_lock);
    return child_process_id;
}

int pm_exit(int process_id, int exit_status) {
    pthread_mutex_lock(&global_process_manager.table_lock);

    process_control_block *current_process = find_process_by_id(process_id);

    if (current_process == NULL || current_process->process_state == ZOMBIE) {
        pthread_mutex_unlock(&global_process_manager.table_lock);
        return -1;
    }

    reparent_children_to_init(current_process);

    current_process->process_state = ZOMBIE;
    current_process->exit_status = exit_status;

    process_control_block *parent_process =
        find_process_by_id(current_process->parent_process_id);
    if (parent_process != NULL) {
        pthread_cond_broadcast(&parent_process->child_exit_condition);
    }

    log_process_snapshot("pm_exit", process_id, exit_status);

    pthread_mutex_unlock(&global_process_manager.table_lock);
    return 0;
}

int pm_wait(int parent_process_id, int child_process_id) {
    pthread_mutex_lock(&global_process_manager.table_lock);

    process_control_block *parent_process = find_process_by_id(parent_process_id);

    if (parent_process == NULL || parent_process->process_state == ZOMBIE) {
        pthread_mutex_unlock(&global_process_manager.table_lock);
        return -1;
    }

    if (!parent_can_wait_for_child(parent_process, child_process_id)) {
        pthread_mutex_unlock(&global_process_manager.table_lock);
        return 0;
    }

    while (true) {
        process_control_block *zombie_child =
            find_zombie_child(parent_process, child_process_id);

        if (zombie_child != NULL) {
            int zombie_exit_status = zombie_child->exit_status;

            detach_child_from_parent(parent_process, zombie_child->process_id);
            process_control_block *removed_process =
                remove_process_by_id(zombie_child->process_id);

            if (parent_process->process_state == BLOCKED) {
                parent_process->process_state = RUNNING;
            }

            log_process_snapshot("pm_wait", parent_process_id, child_process_id);
            pthread_mutex_unlock(&global_process_manager.table_lock);

            destroy_process(removed_process);
            return zombie_exit_status;
        }

        if (!parent_can_wait_for_child(parent_process, child_process_id)) {
            if (parent_process->process_state == BLOCKED) {
                parent_process->process_state = RUNNING;
            }

            pthread_mutex_unlock(&global_process_manager.table_lock);
            return 0;
        }

        parent_process->process_state = BLOCKED;
        log_process_snapshot("pm_wait", parent_process_id, child_process_id);
        pthread_cond_wait(&parent_process->child_exit_condition,
                          &global_process_manager.table_lock);
    }
}

int pm_kill(int process_id) {
    int kill_result = pm_exit(process_id, -9);
    return kill_result;
}

void pm_ps(void) {
    pthread_mutex_lock(&global_process_manager.table_lock);

    process_control_block *sorted_process_list[max_process];
    int process_count = 0;
    build_sorted_process_list(sorted_process_list, &process_count);

    printf("PID\tPPID\tSTATE\t\tEXIT_STATUS\n");
    printf("-----------------------------------------------\n");

    for (int process_index = 0; process_index < process_count; process_index++) {
        process_control_block *current_process = sorted_process_list[process_index];

        if (current_process->process_state == ZOMBIE) {
            printf("%d\t%d\t%-8s\t%d\n",current_process->process_id,current_process->parent_process_id,get_state_name(current_process->process_state),current_process->exit_status);
        } else {
            printf("%d\t%d\t%-8s\t-\n",current_process->process_id,current_process->parent_process_id,get_state_name(current_process->process_state));
        }
    }

    pthread_mutex_unlock(&global_process_manager.table_lock);
}


void run_command(char *input_line) {
    char command_name[32];
    int first_argument = 0;
    int second_argument = 0;

    int parsed_value_count =
        sscanf(input_line, "%31s %d %d", command_name, &first_argument, &second_argument);

    if (parsed_value_count == 2 && strcmp(command_name, "fork") == 0) {
        int child_process_id = pm_fork(first_argument);
        if (child_process_id == -1) {
            printf("[thread %d] fork failed for parent %d\n",
                   current_worker_thread_id, first_argument);
        }
        return;
    }

    if (parsed_value_count == 3 && strcmp(command_name, "exit") == 0) {
        if (pm_exit(first_argument, second_argument) == -1) {
            printf("[thread %d] exit failed for pid %d\n",
                   current_worker_thread_id, first_argument);
        }
        return;
    }

    if (parsed_value_count == 3 && strcmp(command_name, "wait") == 0) {
        int wait_result = pm_wait(first_argument, second_argument);
        if (wait_result == -1) {
            printf("[thread %d] wait failed for parent %d child %d\n",
                   current_worker_thread_id, first_argument, second_argument);
        } else {
            printf("[thread %d] wait returned %d\n",
                   current_worker_thread_id, wait_result);
        }
        return;
    }

    if (parsed_value_count == 2 && strcmp(command_name, "kill") == 0) {
        if (pm_kill(first_argument) == -1) {
            printf("[thread %d] kill failed for pid %d\n",
                   current_worker_thread_id, first_argument);
        }
        return;
    }

    if (parsed_value_count == 2 && strcmp(command_name, "sleep") == 0) {
        usleep(first_argument * 1000);
        return;
    }

    if (parsed_value_count == 1 && strcmp(command_name, "ps") == 0) {
        pm_ps();
        return;
    }

    printf("[thread %d] unknown command: %s", current_worker_thread_id, input_line);
}

void *worker_thread(void *thread_arg) {
    worker_thread_arg *worker_data = (worker_thread_arg *)thread_arg;
    current_worker_thread_id = worker_data->thread_id;

    FILE *script_file_pointer = fopen(worker_data->script_filename, "r");
    if (script_file_pointer == NULL) {
        printf("[thread %d] could not open file %s\n",current_worker_thread_id,worker_data->script_filename);
        return NULL;
    }

    char input_line[MAX_LINE];

    while (fgets(input_line, sizeof(input_line), script_file_pointer) != NULL) {
        if (input_line[0] == '\n' || input_line[0] == '\0') {
            continue;
        }

        if (input_line[0] == '#') {
            continue;
        }

        run_command(input_line);
    }

    fclose(script_file_pointer);
    return NULL;
}

int main(int argument_count, char *argument_values[]) {
    if (argument_count < 2) {
        printf("usage: %s thread0.txt [thread1.txt ...]\n", argument_values[0]);
        return 1;
    }

    snapshot_output_file = fopen("snapshots.txt", "w");
    if (snapshot_output_file == NULL) {
        printf("could not open snapshots.txt\n");
        return 1;
    }

    pm_init();

    pthread_t monitor_thread_handle;
    pthread_create(&monitor_thread_handle, NULL, monitor_thread, NULL);

    int worker_thread_count = argument_count - 1;
    pthread_t worker_thread_handles[worker_thread_count];
    worker_thread_arg worker_arguments[worker_thread_count];

    for (int worker_index = 0; worker_index < worker_thread_count; worker_index++) {
        worker_arguments[worker_index].thread_id = worker_index;
        worker_arguments[worker_index].script_filename =
            argument_values[worker_index + 1];
        pthread_create(&worker_thread_handles[worker_index],NULL,worker_thread,&worker_arguments[worker_index]);
    }

    for (int worker_index = 0; worker_index < worker_thread_count; worker_index++) {
        pthread_join(worker_thread_handles[worker_index], NULL);
    }

    pthread_mutex_lock(&global_process_manager.table_lock);
    should_stop_monitor = 1;
    pthread_cond_signal(&monitor_condition);
    pthread_mutex_unlock(&global_process_manager.table_lock);

    pthread_join(monitor_thread_handle, NULL);

    fclose(snapshot_output_file);
    pm_destroy();
    return 0;
}