import {Component, OnInit} from '@angular/core';
import {HttpClient, HttpHeaders} from "@angular/common/http";
import {Observable} from "rxjs";
import {AppService} from "../../services/app.service";
import {environment} from "../../../environments/environment";
import {Movie} from "../../models/movie";
import { Recommendation } from 'src/app/models/recommendMovie';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.css']
})
export class HomeComponent implements OnInit{
  movies?: Movie[];
  recommendations?: Recommendation[];

  constructor(private appService: AppService, private http: HttpClient) { }

  ngOnInit(): void {
    this.getProducts();
    this.getRecommendations();
  }

  getProducts(): void {
    this.appService.getProducts().subscribe(movies => {
      this.movies = movies;
    });
  }

  getRecommendations(): void {
    this.appService.getRecommendations().subscribe(movies => {
      this.recommendations = movies;
      console.log(this.recommendations);
    });
  }
}
