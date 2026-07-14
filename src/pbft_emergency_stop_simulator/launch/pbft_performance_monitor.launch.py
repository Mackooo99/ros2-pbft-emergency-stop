"""Launch only the PBFT performance measurement node."""

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node
from launch_ros.parameter_descriptions import ParameterValue


def generate_launch_description() -> LaunchDescription:
    """Create the performance-monitor launch description."""
    arguments = [
        DeclareLaunchArgument('replica_count', default_value='4'),
        DeclareLaunchArgument('max_faulty', default_value='1'),
        DeclareLaunchArgument(
            'scenario_label',
            default_value='manual',
        ),
        DeclareLaunchArgument(
            'faulty_nodes',
            default_value='none',
        ),
        DeclareLaunchArgument(
            'faulty_behaviors',
            default_value='none',
        ),
        DeclareLaunchArgument(
            'output_csv',
            default_value=(
                '~/.ros/pbft_performance/'
                'performance_results.csv'
            ),
        ),
        DeclareLaunchArgument(
            'output_markdown',
            default_value=(
                '~/.ros/pbft_performance/'
                'performance_results.md'
            ),
        ),
        DeclareLaunchArgument(
            'output_jsonl',
            default_value=(
                '~/.ros/pbft_performance/'
                'performance_results.jsonl'
            ),
        ),
        DeclareLaunchArgument(
            'measurement_timeout_sec',
            default_value='30.0',
        ),
        DeclareLaunchArgument(
            'finalize_delay_sec',
            default_value='0.5',
        ),
        DeclareLaunchArgument(
            'request_id_filter',
            default_value='',
        ),
        DeclareLaunchArgument(
            'truncate_output_on_start',
            default_value='false',
        ),
        DeclareLaunchArgument(
            'log_each_protocol_message',
            default_value='false',
        ),
    ]

    monitor = Node(
        package='pbft_emergency_stop_simulator',
        executable='performance_monitor',
        name='pbft_performance_monitor',
        output='screen',
        emulate_tty=True,
        parameters=[
            {
                'replica_count': ParameterValue(
                    LaunchConfiguration('replica_count'),
                    value_type=int,
                ),
                'max_faulty': ParameterValue(
                    LaunchConfiguration('max_faulty'),
                    value_type=int,
                ),
                'scenario_label': LaunchConfiguration(
                    'scenario_label'
                ),
                'faulty_nodes': LaunchConfiguration(
                    'faulty_nodes'
                ),
                'faulty_behaviors': LaunchConfiguration(
                    'faulty_behaviors'
                ),
                'output_csv': LaunchConfiguration(
                    'output_csv'
                ),
                'output_markdown': LaunchConfiguration(
                    'output_markdown'
                ),
                'output_jsonl': LaunchConfiguration(
                    'output_jsonl'
                ),
                'measurement_timeout_sec': ParameterValue(
                    LaunchConfiguration(
                        'measurement_timeout_sec'
                    ),
                    value_type=float,
                ),
                'finalize_delay_sec': ParameterValue(
                    LaunchConfiguration(
                        'finalize_delay_sec'
                    ),
                    value_type=float,
                ),
                'request_id_filter': LaunchConfiguration(
                    'request_id_filter'
                ),
                'truncate_output_on_start': ParameterValue(
                    LaunchConfiguration(
                        'truncate_output_on_start'
                    ),
                    value_type=bool,
                ),
                'log_each_protocol_message': ParameterValue(
                    LaunchConfiguration(
                        'log_each_protocol_message'
                    ),
                    value_type=bool,
                ),
            }
        ],
    )

    return LaunchDescription(arguments + [monitor])
