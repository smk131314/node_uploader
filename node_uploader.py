import argparse
import os
from PIL import Image
from PIL.ExifTags import TAGS
import psycopg2

def main():
    # parser 객체 생성 시 description으로 코드에 대한 소개 작성
    parser = argparse.ArgumentParser(description='This code is written for node uploader')

    # 입력 받고자하는 명령행 옵션(인자의 조건) 지정
    # metavar 로 인자의 이름 명시
    # 경로
    parser.add_argument('input', type=str,
                        metavar='image path',
                        help='path?')
    # link upload 옵션
    # action = 'store_true' 으로 인자값 입력하지 않아도 기본값 True로 저장한다.
    # store_true 액션을 사용할 경우, 불린값이기 때문에 type 사용하면 안된다.
    parser.add_argument('-l',
                        action='store_true',
                        # metavar='link upload option',
                        help='upload node with link')

    # 인자 파싱해서 args에 저장
    args = parser.parse_args()

    path = args.input
    link = args.l

    # node uploader 객체 생성
    nu = NodeUploader()
    # connect pg
    nu.open()

    # main process
    # nu.search(path)
    nu.main(path, link)

    # disconnect pg
    nu.close()


class NodeUploader:
    # init 함수 생략, 접속 오류 확인하기 위해 DB 접속 함수를 따로 분리해줌.

    # DB 접속
    def open(self, db="seoul_alley", user="wemap", host="192.168.0.9", password="wemap"):
        try:
            self.conn = psycopg2.connect(database=db, user=user, host=host, password=password)
            self.cur = self.conn.cursor()
        except Exception as e:
            raise(e)

    # def query(self, query):
    #     self.cur.execute(query)
    #     self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()

    # main process
    def main(self, path, link):
        # node 입력함수 실행
        self.search(path)

        # link == True 이면 link upload 실행
        if link:
            self.link_uploader(path)


    # 디렉토리 파일 열거 및 node upload
    def search(self, p):
        # 권한이 없는 디렉토리에 접근하더라도 그냥 수행하기 위해 try ... except문 사용
        try:
            # os.walk로 하위디렉토리까지 검색
            for (path, dir, files) in os.walk(p):
                for filename in files:
                    # 확장자를 기준으로 파일이름 두 부분 split, [-1]이면 파일의 확장자 이름
                    ext = os.path.splitext(filename)[-1]
                    # 확장자가 jpg인 파일만 출력
                    # should filter blur file ('.blur' postfix in filename) -> ignore the item
                    if ext == '.jpg':
                        # 전체 파일 경로 join
                        full_path = os.path.join(path, filename)
                        # print(full_path)

                        # upload
                        self.upload(full_path)
        except PermissionError:
            pass


    # upload
    def upload(self, path):
        # read exif
        geom = self.get_coordinates(path)
        # insert into alley_node
        self.pg_uploader(path, geom)

        # test
        # print(geom)

    # exif에서 lat, lon 추출하는 함수
    def get_coordinates(self, path):
        image = Image.open(path)
        # exif 정보 얻기
        info = image._getexif();
        image.close()

        # TAG이름을 읽기 쉽게 변경해준다.
        taglabel = {}
        for tag, value in info.items():
            # TAG decimal 코드를 TAG 키네임으로 변경
            decoded = TAGS.get(tag, tag)
            taglabel[decoded] = value

        exifGPS = taglabel['GPSInfo']
        latData = exifGPS[2]
        lonData = exifGPS[4]
        # 고도, float으로 형변환
        altitude = float(exifGPS[6])

        # 촬영 시간
        exifTime = taglabel['DateTimeOriginal']

        # 도, 분, 초 계산
        latDeg, latMin, latSec = latData[0], latData[1], latData[2]
        lonDeg,lonMin, lonSec = lonData[0], lonData[1], lonData[2]


        # 도 decimal로 나타내기
        # 위도 계산
        Lat = (latDeg + (latMin + latSec / 60.0) / 60.0)
        # 북위, 남위인지를 판단, 남위일 경우 -로 변경
        if exifGPS[1] == 'S': Lat = Lat * -1

        # 경도 계산
        Lon = (lonDeg + (lonMin + lonSec / 60.0) / 60.0)
        # 동경, 서경인지를 판단, 서경일 경우 -로 변경
        if exifGPS[3] == 'W': Lon = Lon * -1

        # 촬영방향 정보
        heading = 0

        return (Lon, Lat, heading, altitude, exifTime)

    # DB에 업로드
    def pg_uploader(self, path, geometry):
        # path, geom 정보 입력
        splited_path = path.split('\\')
        # full_path = path
        # \ -> / 으로 수정
        full_path = path.replace('\\', '/')
        # TO DO : 파일명 규칙에 따라 다시 parsing 필요
        dir_path = '/'.join(splited_path[0:-2])
        dir_path = dir_path + '/'
        # dir_path = '\\'.join(splited_path[0:-1])
        file_nm = splited_path[-1]
        geom = geometry
        lng, lat, heading, altitude, time = geom[0], geom[1], geom[2], geom[3], geom[4]

        try:
            # 쿼리 실행 (https://www.psycopg.org/docs/usage.html)
            self.cur.execute("""
                Insert INTO alley_node (full_path, heading, geom, status, dir_path, file_nm, file_time, altitude)
                 VALUES (%(full_path)s, %(heading)s, ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 3857), 'original', 
                        %(dir_path)s, %(file_nm)s, to_timestamp(%(file_time)s, 'YYYY:MM:DD HH24:MI:SS'), %(altitude)s)
                 ON CONFLICT (full_path) DO nothing
                 RETURNING file_nm;      
            """,
                {'full_path': full_path, 'heading': heading, 'lng': lng, 'lat': lat, 'dir_path': dir_path,'file_nm': file_nm,
                 'file_time': time, 'altitude': altitude})
        except Exception as e:
            print(e)

        # 결과 출력
        res = self.cur.fetchone()[0]
        print("file '", res, "' successfully uploaded!")

        self.conn.commit()



    # link 자동 입력 함수
    def link_uploader(self, path):
        # TO DO : 파일명 규칙에 따라 다시 parsing 필요 : 일단 '날짜/조이름' 으로 입력받는다고 가정하고 작성함.
        # dir_path = path
        dir_path = path if path[-1] == '/' else (path + '/')
        try:
            # 쿼리 실행
            self.cur.execute("""
                WITH frto AS ( 
                    SELECT node_id as cur_id, file_time, 
                           LEAD(node_id) OVER(ORDER BY file_time) AS next_id,
                           geom AS cur_geom,
                           LEAD(geom) OVER(ORDER BY file_time) AS next_geom
                      FROM alley_node 
                      WHERE dir_path = %(dir_path)s
                )
                INSERT INTO alley_link (node_fr, node_to, geom)
                    (SELECT cur_id, next_id, ST_setSRID(ST_makeline(cur_geom, next_geom), 3857)
                    FROM frto WHERE next_id NOTNULL) ON CONFLICT (least(node_fr, node_to), greatest(node_fr, node_to)) DO NOTHING
                RETURNING link_id; 
            """, {'dir_path': dir_path})
        except Exception as e:
            print(e)

        # 결과 출력
        res = self.cur.fetchall()
        print("alley_link's updated rows: ", len(res))
        for i in res:
            print("Link id: ", i[0], " successfully created!")

        self.conn.commit()


if __name__ == "__main__":
    main()


