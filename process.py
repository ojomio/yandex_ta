# coding=utf-8
import math
import sys
from collections import defaultdict


def main():
    frontend_request_status, replics_stat, incomplete_requests, input_name, output_name = init()
    with open(input_name) as input_file:
        for line in input_file:
            line = line.strip()
            timestamp, front_req_id, other = line.split('\t', 2)
            timestamp = int(timestamp)
            if '\t' in other:  # Если в строке есть доп информация после типа события
                event_type, add_info = other.split('\t', 1)
            else:
                event_type = other
                add_info = None

            request_stat = frontend_request_status[front_req_id]

            if event_type == 'StartRequest':
                request_stat['start'] = timestamp

            if event_type in ('BackendConnect', 'BackendError', 'BackendOk'):
                # Получим номер ГР и её статистику - общую и в данном запросе
                replica_group = add_info.split('\t')[0]
                replica_total_stat = replics_stat[replica_group]
                replica_per_req_stat = request_stat['replic_group_info'][replica_group]

                if event_type == 'BackendConnect':
                    backend_url = add_info.split('\t')[1]
                    replica_total_stat[backend_url]['hits'] += 1

                    replica_per_req_stat['current_backends_queue'].insert(0, backend_url)

                    # Если запрос хотя бы от одного бэкэнда ГР удачен, опрос ГР успешен и мы не меняем её статус
                    # Если мы уже опрашиваем другой бэкэнд это ГР - тоже
                    if replica_per_req_stat['status'] is None:
                        # Ждём ответа от бэкэндов этой ГР, если до StartMerge не ответят - запрос неполный
                        replica_per_req_stat['status'] = 'pending'

                if event_type == 'BackendError':
                    error = add_info.split('\t')[1]
                    # Будем считать, что ответы от бэкэндов ГР приходят в том же порядке, что и запросы к ним,
                    # иначе их невозможно идентифицировать
                    backend_url = replica_per_req_stat['current_backends_queue'].pop()

                    replica_total_stat[backend_url]['errors'][error] += 1

                if event_type == 'BackendOk':
                    # Хотя бы один бэкэнд это ГР ответил
                    replica_per_req_stat['current_backends_queue'].pop()
                    replica_per_req_stat['status'] = 'complete'

            if event_type == 'StartMerge':
                if any([x['status'] == 'pending' for x in request_stat['replic_group_info'].itervalues()]):
                    incomplete_requests += 1

                del request_stat['replic_group_info']  # Экономим память

            if event_type == 'StartSendResult':
                request_stat['start_resp'] = timestamp

            if event_type == 'FinishRequest':
                request_stat['end'] = timestamp
                request_stat['duration_total'] = request_stat['end'] - request_stat['start']
                request_stat['duration_resp'] = request_stat['end'] - request_stat['start_resp']

    print_result(frontend_request_status, incomplete_requests, replics_stat, output_name)


def init():
    # Статистика по каждой группе реплик индексируется номером группы
    # Внутри группы статистика по бэкэндам индексируется URL бэкэнда
    # Внутри бэкэнда значения по умолчанию
    replics_stat = defaultdict(
        lambda: defaultdict(
            lambda: {
                'hits': 0,
                'errors': defaultdict(lambda: 0),
            }
        )
    )
    # Статистика по фронтенд-запросам индексируется номером запроса и
    # хранит для каждого запроса данные о группах реплик, к которым производился запрос,
    # + информация о длительности этапов соединения

    # Информация о группах реплик индексируется по номеру ГР и
    # содержит список запрашиваемых на данный момент бэкэндов одной ГР(в 99% - только один бэкэнд)
    # а также итог опроса каждой ГР на текущий момент

    frontend_request_status = defaultdict(
        lambda: {
            'replic_group_info': defaultdict(
                lambda: {
                    'current_backends_queue': [],
                    'status': None,
                }
            )
        }
    )
    incomplete_requests = 0
    input_name = 'input.txt'
    output_name = 'output.txt'
    if len(sys.argv) > 1:
        input_name = sys.argv[1]
        if len(sys.argv) > 2:
            output_name = sys.argv[2]

    return frontend_request_status, replics_stat, incomplete_requests, input_name, output_name


def print_result(frontend_request_status, incomplete_requests, replics_stat, output_name):
    with open(output_name, 'w') as output_file:
        # Посчитаем 95% перцентиль
        sorted_processing_times = sorted([
                                             req_info['duration_total']
                                             for req_info
                                             in frontend_request_status.itervalues()]
                                         )
        idx = int(math.ceil(0.95 * len(frontend_request_status)) - 1)
        output_file.write('По доле: 95%% перцентиль:\n\t%.2f\n' % sorted_processing_times[idx])

        # Самые длинные запросы
        slowest_user_response = sorted(
            frontend_request_status.items(),
            key=lambda x: x[1]['duration_resp'],
            reverse=True
        )
        output_file.write('Запросы с самым длинным времененм ответа пользователю:\n')
        output_file.write('\t%s\n' % [x[0] for x in slowest_user_response[:10]])

        # Статистика ошибок и подключений по бэкэндам
        for replica_group, replica_stat in sorted(replics_stat.items(), key=lambda x: x[0]):
            output_file.write('Группа реплик #%s\n' % replica_group)
            for be_id, backend_info in sorted(replica_stat.iteritems(), key=lambda x: x[0]):
                output_file.write('\t%s:\n' % be_id)
                output_file.write('\t\tОбращений: %d\n' % backend_info['hits'])
                if backend_info['errors']:
                    output_file.write('\t\tСводка ошибок:\n')
                    for error, error_count in backend_info['errors'].iteritems():
                        output_file.write('\t\t\t%s: %d\n' % (error, error_count))

        # Неполные запросы
        output_file.write('Запросов с неполным набором групп реплик:\n\t%d\n' % incomplete_requests)


if __name__ == '__main__':
    main()
