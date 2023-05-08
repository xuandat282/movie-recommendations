import {Injectable} from '@angular/core';
import {Observable} from "rxjs";
import {HttpClient} from "@angular/common/http";
import {environment} from "../../environments/environment";
import {Movie} from "../models/movie";
import { Recommendation } from '../models/recommendMovie';

@Injectable({
  providedIn: 'root'
})
export class AppService {

  constructor(private http: HttpClient) { }

  getProducts(): Observable<Movie[]> {
    return this.http.get<Movie[]>(`${environment.apiUrl}/api/top/movie/top-n/10`);
  }

  getRecommendations(): Observable<Recommendation[]> {
    return this.http.get<Movie[]>(`${environment.apiUrl}/api/rec_engine/pagerank/user2/10`);
  }
}
