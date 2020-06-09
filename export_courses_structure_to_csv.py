import csv
import os

from opaque_keys.edx.keys import CourseKey
from xmodule.modulestore.django import modulestore


csv_result_dir = '/tmp/result/'


def write_row(num, chapter_block, seq_block, vert_block, item, grading_policy, csv_writer):
    is_proctored_exam = 'y' if getattr(seq_block, 'is_proctored_exam', False) else ''
    graded = 'y' if vert_block.format else ''
    grading_type = vert_block.format if graded else ''
    grading_type_abs_weight = grading_policy.get(grading_type)['weight'] * 100 if grading_type else ''
    grading_type_passing_grade = ''
    if grading_type:
        grading_type_passing_grade = grading_policy.get(grading_type, {}).get('passing_grade', 0)
    visible = '' if vert_block.visible_to_staff_only else 'y'

    try:
        csv_writer.writerow([
            str(num),
            str(item.location),
            str(vert_block.location),
            str(seq_block.location),
            str(chapter_block.location),
            str(item.category),
            item.display_name.encode("utf-8") if item.display_name else '',
            vert_block.display_name.encode("utf-8") if vert_block.display_name else '',
            seq_block.display_name.encode("utf-8") if seq_block.display_name else '',
            chapter_block.display_name.encode("utf-8") if chapter_block.display_name else '',
            visible,
            graded,
            grading_type,
            str(vert_block.weight),
            str(grading_type_abs_weight),
            str(grading_type_passing_grade),
            is_proctored_exam
        ])
    except:
        print item.display_name, vert_block.display_name, seq_block.display_name, chapter_block.display_name
        raise


def create_course_csv(course_id, csv_writer):
    course_key = CourseKey.from_string(course_id)
    course = modulestore().get_course(course_key)
    grading_policy = {}

    for v in course.grading_policy['GRADER']:
        grading_policy[v['type']] = v.copy()

    csv_writer.writerow([
        'Num',
        'Block ID',
        'Vertical ID',
        'Section ID',
        'Chapter ID',
        'Block Type',
        'Block Title',
        'Vertical Title',
        'Section Title',
        'Chapter Title',
        'Visibility',
        'Graded',
        'Grading Type',
        'Weight',
        'Grading Type Abs Weight',
        'Grading Type Passing Grade',
        'Proctored'
    ])

    num = 1

    for chapter_block in course.get_children():
        for seq_block in chapter_block.get_children():
            for vert_block in seq_block.get_children():
                for item in vert_block.get_children():
                    if item.category == 'library_content':
                        for subitem in item.get_children():
                            write_row(num, chapter_block, seq_block, vert_block, subitem, grading_policy, csv_writer)
                            num = num + 1
                    else:
                        write_row(num, chapter_block, seq_block, vert_block, item, grading_policy, csv_writer)
                        num = num + 1


all_courses = modulestore().get_courses()
for course in all_courses:
    run = str(course.location.run)
    course_id = str(course.id)
    if run in ('2018', '2019', '2020'):
        csv_file_name = csv_result_dir + course_id.split(':')[1].replace('+', '_') + '.csv'

        if not os.path.isfile(csv_file_name):
            try:
                with open(csv_file_name, 'w') as f:
                    csv_writer = csv.writer(f, delimiter=';')
                    print 'Create CSV for course: ', course_id
                    create_course_csv(course_id, csv_writer)
            except:
                os.remove(csv_file_name)
                raise
        else:
            print 'Skip course: ', course_id

